package dir

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	leveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"net"
	"rcstor/common"
	"sort"
	"sync"
	"time"
)

type Task struct {
	MethodName string
	Args  interface{}
}


type DirectoryService struct {
	Volumes sync.Map

	db		*leveldb.DB
	tasks	chan Task
	port    uint16

	initialized bool
	lock sync.RWMutex
}

func translateIP(servers []ServerInfo) {
	for i,server := range servers {
		ips,err := net.LookupIP(server.IP)
		if err != nil || len(ips)<1 {
			log.Errorln("Unable to resolve ip ",server.IP,err)
		}
		for _,ip := range ips {
			if ip.To4() != nil {
				servers[i].IP = ip.To4().String()
			}
		}

	}
}

var writeOperations = opt.WriteOptions{Sync: true}

func (service *DirectoryService) Close() {
	service.db.Close()
}

type CreateVolumeArgs struct {
	VolumeName string `json:"VolumeName"`
	Servers    []ServerInfo `json:"Servers"`
	Parameter  VolumeParameter `json:"VolumeParameter"`
}

func (service *DirectoryService) CreateVolume(args *CreateVolumeArgs,reply *interface{}) error{

	volumeName := args.VolumeName
	servers := args.Servers
	parameter := args.Parameter

	translateIP(servers)

	volume := &Volume{VolumeName: volumeName,Servers: servers,Version: 0,Parameter: parameter}

	volume.Bricks = make(map[uuid.UUID]*Brick)
	volume.HttpServers = make([]HttpServer,len(volume.Servers))

	_,exist := service.Volumes.LoadOrStore(volumeName,volume)

	if exist {
		return common.ErrVolumeExists
	}

	content,err := json.Marshal(volume)
	if err != nil {
		log.Errorln(err)
	}

	if err != nil {
		return err
	}
	err = service.db.Put([]byte("volume#"+volumeName),content,&writeOperations)
	if err!= nil { return err}

	return err
}

func (service *DirectoryService) myAddr(volume *Volume) string {
	myIP := common.GetOutboundIP(volume.Servers[0].IP)
	return fmt.Sprintf("%s:%d",myIP.String(),service.port)
}

func (service *DirectoryService) StartVolume(name *string,reply *interface{}) error{
	service.waitForInitialization()

	volumeName := *name

	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.Lock()

	if volume.started == true{
		volume.mu.Unlock()
		return common.ErrVolumeStarted
	}
	volume.started = true
	if volume.Bricks == nil {
		volume.Bricks = make(map[uuid.UUID]*Brick)
	}


	myAddr := service.myAddr(volume)

	var wg sync.WaitGroup
	wg.Add(len(volume.Servers))
	for serverId,server := range volume.Servers {
		go func(serverId int,info ServerInfo){
			startServerBricks(info,volumeName,myAddr)
			startHTTP(volumeName,info.IP,myAddr)
			wg.Done()
		}(serverId,server)
	}
	wg.Wait()

	volume.mu.Unlock()

	now := time.Now()

	for time.Since(now) < common.RegisterTimeout{
		time.Sleep(time.Millisecond * 300)
		if volume.isHTTPRegistered() && volume.isBrickRegistered() {
			volume.mu.Lock()
			defer volume.mu.Unlock()
			service.autoStart(volume)
			err := service.db.Put([]byte("started#"+volumeName),[]byte("yes"),&writeOperations)
			if err!= nil { return err}
			if volume.PGs == nil {
				volume.assignPGs()

				content,err := json.Marshal(volume)
				if err != nil {
					log.Errorln(err)
				}

				if err != nil {
					return err
				}
				err = service.db.Put([]byte("volume#"+volumeName),content,&writeOperations)
				if err!= nil { return err}

			}
			return nil
		}
	}


	return common.ErrTimeOut
}

func (service *DirectoryService) GetVolume(name *string,reply *Volume) error{
	service.waitForInitialization()

	volumeName := *name
	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	//Should be deep copy to avoid concurrent access
	(*reply).Bricks = map[uuid.UUID]*Brick{}
	for k,v := range volume.Bricks{
		(*reply).Bricks[k] = v
	}

	(*reply).PGs = volume.PGs
	(*reply).HttpServers = volume.HttpServers
	(*reply).Servers = volume.Servers

	(*reply).VolumeName = volume.VolumeName
	(*reply).Version = volume.Version
	(*reply).Parameter = volume.Parameter

	return nil
}


func (service *DirectoryService) StopVolume(name *string,reply *interface{}) error{
	service.waitForInitialization()

	volumeName := *name

	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.Lock()
	defer volume.mu.Unlock()

	volume.started = false


	var wg sync.WaitGroup
	wg.Add(len(volume.Servers))
	for _,server := range volume.Servers {
		go func(server ServerInfo) {
			volume.stopStorageServer(server)
			wg.Done()
		}(server)
	}
	for _,server := range volume.HttpServers {
		if server.IsRegistered() {
			wg.Add(1)
			go func(IP string, pid int) {
				volume.stopHttpServer(IP, pid)
				wg.Done()
			}(server.IP, server.PID)
		}
	}
	wg.Wait()

	volume.Bricks = nil
	volume.HttpServers = nil
	err := service.db.Delete([]byte("started#"+volumeName),&writeOperations)

	return err
}

func (service *DirectoryService) DropVolume(name *string,reply *interface{}) error {
	service.waitForInitialization()

	volumeName := *name

	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)

	volume.mu.RLock()
	if volume.started == false{
		defer volume.mu.RUnlock()
		return common.ErrVolumeNotStarted
	}
	volume.mu.RUnlock()

	brickPool := MakeVolumeConnectionPool(volumeName,common.MakeConnection(service.myAddr(volume)))

	//FIXME: what if volume is stopping? the service will be blocked.
	//volume.mu.Lock()
	//defer volume.mu.Unlock()

	log.Println("Begin to drop",volumeName)


	var wg sync.WaitGroup
	wg.Add(len(volume.Bricks))
	for brickId,_ := range volume.Bricks {
		go func(id uuid.UUID) {
			conn := brickPool.GetConnectionByBrick(id)
			var args int
			var reply interface{}
			err := conn.Call("IndexService.Drop", &args, &reply)
			if err != nil{
				log.Errorln(err)
			}
			wg.Done()
		}(brickId)
	}
	wg.Wait()

	brickPool.Release()
	err := service.StopVolume(&volumeName,nil)

	if err != nil {
		return err
	}
	service.Volumes.Delete(volumeName)
	err = service.db.Delete([]byte("volume#"+volumeName),&writeOperations)
	if err != nil {
		return err
	}

	return nil
}

func (service *DirectoryService) GetAllVolumes(args *int,reply *[]Volume) error {
	service.waitForInitialization()

	*reply = make([]Volume,0)

	service.Volumes.Range(func(k,v interface{}) bool {
		*reply = append(*reply,*(v.(*Volume)))
		return true
	})

	sort.Slice(*reply,func(i,j int) bool {
		return (*reply)[i].VolumeName < (*reply)[j].VolumeName
	})

	return nil
}

func (service *DirectoryService) GetPGs(volumeName string, pgs *[]PlacementGroup) error {
	service.waitForInitialization()

	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	*pgs = volume.PGs
	return nil
}

func (service *DirectoryService) GetAllBricks(volumeName string, servers *[]ServerInfo) error {
	service.waitForInitialization()

	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	*servers = volume.Servers
	return nil
}

type BrickRegisterArgs struct {
	BrickID uuid.UUID
	Port uint16
	Dir  string
	VolumeName string
	IP        string
	PID       int
}

type BrickRegisterReply struct {
	layout   common.Layout
}

//Not thread safe
func (service *DirectoryService) autoStart(volume *Volume) {

		volume.started = true
		go func() {
			myAddr := service.myAddr(volume)
			for {
				time.Sleep(common.HeartBeatInterval)
				volume.mu.Lock()

				if !volume.started {
					volume.mu.Unlock()
					break
				}

				var wg sync.WaitGroup
				for serverId,server := range volume.Servers {
					for i:=0;i<len(server.Dir);i++ {
						registered := false
						var brick *Brick
						if server.BrickId != nil && server.BrickId[i] != uuid.Nil {
							brick,registered = volume.Bricks[server.BrickId[i]]
							if registered && time.Since(brick.LastPingTime) > common.HeartBeatInterval * 4 {
								wg.Add(1)
								go func(brick *Brick) {
									restartBrick(brick,volume.VolumeName,myAddr)
									wg.Done()
								}(brick)
							}
						}
						if !registered {
							wg.Add(1)
							go func(server ServerInfo,i int) {
								startBrick(server.Dir[i],server.IP,volume.VolumeName,myAddr)
								wg.Done()
							}(server,i)
						}
					}
					if len(volume.HttpServers) <= serverId || !volume.HttpServers[serverId].IsRegistered(){
						wg.Add(1)
						go func(serverId int,server ServerInfo) {
							startHTTP(volume.VolumeName,server.IP,myAddr)
							wg.Done()
						}(serverId,server)
					} else if time.Since(volume.HttpServers[serverId].LastPingTime) > common.HeartBeatInterval * 4 {
						wg.Add(1)
						go func(serverId int,server *HttpServer) {
							restartHttp(server,volume.VolumeName,myAddr)
							wg.Done()
						}(serverId,&volume.HttpServers[serverId])
					}

				}
				volume.mu.Unlock()
				wg.Wait()
			}
		}()
}

func (volume *Volume) isBrickRegistered() bool{
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	bricks := 0
	for _,server:= range volume.Servers {
		bricks += len(server.Dir)
	}
	return bricks == len(volume.Bricks)
}

func (volume *Volume) isHTTPRegistered() bool{
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	if volume.HttpServers == nil {
		return false
	}
	for _,server:= range volume.HttpServers {
		if !server.IsRegistered(){
			return false
		}
	}
	return true
}

func (service *DirectoryService) BrickRegister(args *BrickRegisterArgs,reply *VolumeParameter) error{

	loaded, ok := service.Volumes.Load(args.VolumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.Lock()
	defer volume.mu.Unlock()

	if volume.Bricks == nil {
		volume.Bricks = make(map[uuid.UUID]*Brick)
	}
	if !volume.started{
		return nil
	}

	*reply = volume.Parameter

	if args == nil || args.BrickID == uuid.Nil{
		return nil
	}

	brick := &Brick{}
	brick.UUID = args.BrickID
	brick.Port = args.Port
	brick.IP = args.IP
	brick.Dir = args.Dir
	brick.PID = args.PID
	brick.LastPingTime = time.Now()

	volume.Bricks[brick.UUID] = brick


	for i:=0;i<len(volume.Servers);i++ {
		if volume.Servers[i].IP == brick.IP {
			if volume.Servers[i].BrickId == nil {
				volume.Servers[i].BrickId = make([]uuid.UUID,len(volume.Servers[i].Dir))
			}
			for j:=0;j<len(volume.Servers[i].Dir);j++ {
				if volume.Servers[i].Dir[j] == brick.Dir {
					volume.Servers[i].BrickId[j] = brick.UUID
					return nil
				}
			}
		}
	}

	log.Errorln("Server not found")
	return nil
}

type HttpRegisterArgs struct {
	IP        string
	Port      uint16
	ClientPort uint16
	PID        int
	VolumeName string
}

type HttpRegisterReply struct {
	ClientId   int
	NumClients int
}

func (service *DirectoryService) HttpRegister(args *HttpRegisterArgs,reply *HttpRegisterReply) error{

	loaded, ok := service.Volumes.Load(args.VolumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	//log.Infoln("Http register received")
	volume.mu.Lock()
	//log.Infoln("Mutex locked for Http server")
	defer volume.mu.Unlock()

	if !volume.started{
		return nil
	}
	//log.Infoln(fmt.Sprintf("Http Server %s:%d",args.IP,args.Port),"is registerd")
	if volume.HttpServers == nil {
		volume.HttpServers = make([]HttpServer,len(volume.Servers))
	}

	server := &HttpServer{}

	if args != nil {
		server.PID = args.PID
		server.IP = args.IP
		server.Port = args.Port
		server.ClientPort = args.ClientPort
		server.LastPingTime = time.Now()
	}

	for i:=0;i<len(volume.Servers);i++ {
		if volume.Servers[i].IP == server.IP {
			//if volume.HttpServers[i].IsRegistered() {
			//	common.ResetConnectionAddr(volume.HttpServers[i].ClientAddr(),server.ClientAddr())
			//}
			if args != nil {
				volume.HttpServers[i] = *server
			}
			reply.ClientId = i
			reply.NumClients = len(volume.Servers)
			return nil
		}
	}

	log.Errorln("Server not found")
	return nil
}

func (service *DirectoryService) waitForInitialization() {
	for !service.isInitialized(){
		time.Sleep(time.Second)
	}
}

func (service *DirectoryService) isInitialized() bool{
	service.lock.RLock()
	defer service.lock.RUnlock()
	return service.initialized
}

func (service *DirectoryService) startUp(dbPath string, port uint16) {
	gob.Register(PlacementGroup{})
	db, err := leveldb.OpenFile(dbPath,nil)
	if err != nil {
		log.Error(err)
	}
	service.db = db
	service.port = port
	service.tasks = make(chan Task,1000)

	iter := db.NewIterator(util.BytesPrefix([]byte("volume#")), nil)
	for iter.Next() {
		// Use key/value.
		volumeName := string(iter.Key()[7:])
		volume := &Volume{}

		err := json.Unmarshal(iter.Value(),volume)
		if err!=nil {
			log.Errorln(err)
		}

		volume.Bricks = make(map[uuid.UUID]*Brick)
		volume.HttpServers = make([]HttpServer,len(volume.Servers))

		service.Volumes.Store(volumeName,volume)

		started,err := db.Has([]byte("started#"+volumeName),nil)

		if err != nil {
			log.Errorln(err)
		}
		if started {
			service.autoStart(volume)
		}
	}
	iter.Release()
	service.initialized = false

	//only heart beat registeration can be accepted.
	service.startServer(port)

	service.Volumes.Range(func(volumeName,volume interface{}) bool {
		v := volume.(*Volume)
		if v == nil{
			return false
		}
		if v.started == false {
			return true
		}
		log.Infoln("Waiting for the registeration of",volumeName)
		now := time.Now()
		for time.Since(now) < common.RegisterTimeout{
			time.Sleep(time.Second)
			if v.isHTTPRegistered() && v.isBrickRegistered() {
				return true
			}
		}
		return false
	})

	service.lock.Lock()
	service.initialized = true
	service.lock.Unlock()
	log.Infoln("Initialization finished.")
}

func (service *DirectoryService) showPGs(volumeName string) error{
	loaded, ok := service.Volumes.Load(volumeName)
	if!ok{
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	for id, pg := range volume.PGs {
		log.Infof("pg_%d version:%d", id, pg.Version)
		for _, brickID := range pg.Bricks {
			brick := volume.Bricks[brickID]
			log.Infof("Brick %s %s:%d/%s", brick.UUID.String(),brick.IP, brick.Port, brick.Dir)
		}
		log.Infoln("-----------------")
	}
	return nil
}

func (service *DirectoryService) startServer(port uint16) {
	addr := "0.0.0.0:" + fmt.Sprintf("%d", port)
	s, l, _ := common.StartRPCServer(addr)
	s.Register(service)

	listener, err := net.ListenTCP("tcp", l)
	if err != nil {
		log.Fatalln(err)
	}

	go common.ServeRPCServer(s, listener,nil)
}


func (volume *Volume) checkStatus() {

}


func StartDirectoryService(dbPath string,port uint16) *DirectoryService {
	dir := &DirectoryService{}
	dir.startUp(dbPath,port)
	go dir.serveTasks()
	return dir
}
