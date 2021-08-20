package dir

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"math/rand"
	"rcstor/common"
	"sync"
	"time"
)



func (service *DirectoryService) callClientsMethods(volumeName string, methodName string,clientId int) error {
	service.waitForInitialization()

	loaded, ok := service.Volumes.Load(volumeName)
	if !ok {
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)

	var wg sync.WaitGroup

	volume.mu.RLock()
	nClients := len(volume.Servers)

	myAddr := service.myAddr(volume)
	volume.mu.RUnlock()

	dirConn := common.MakeConnection(myAddr)
	pool := MakeVolumeConnectionPool(volumeName, dirConn)

	logrus.Infoln("Begin to", methodName)
	begin := time.Now()

	if clientId == -1 {
		wg.Add(nClients)
		for serverId := 0; serverId < nClients; serverId++ {
			go func(serverId int) {
				var args interface{}
				var reply interface{}
				conn := pool.GetClientConnectionById(serverId)
				err := conn.Call("Client."+methodName, &args, &reply)
				if err != nil {
					logrus.Errorln(err)
				}
				wg.Done()
			}(serverId)
		}
		wg.Wait()
	} else {
		var args interface{}
		var reply interface{}
		conn := pool.GetClientConnectionById(clientId)
		err := conn.Call("Client."+methodName, &args, &reply)
		if err != nil {
			logrus.Errorln(err)
		}
	}
	logrus.Println(methodName, "has finished.","Time cost:",time.Since(begin))

	pool.Release()
	dirConn.Close()

	return nil
}

func (service *DirectoryService) serveTasks() {
	for task := range service.tasks {
		switch task.MethodName {
		case "TracePuts", "GenerateParity", "TraceDgets":
			err := service.callClientsMethods(*(task.Args.(*string)), task.MethodName,-1)
			if err != nil {
				logrus.Errorln(err)
			}
			break

		case "Recovery":
			err := service.Recovery(task.Args.(*RecoveryDispatchArgs),nil)
			if err != nil{
				logrus.Errorln(err)
			}
		}
	}
}

func (service *DirectoryService) TracePuts(name *string, reply *interface{}) error {

	err := service.callClientsMethods(*name, "TracePuts",-1)
	if err != nil {
		logrus.Errorln(err)
	}
	return err
	//
	//task := Task{MethodName: "TracePuts", Args: name}
	//service.tasks <- task

}


func (service *DirectoryService) ForegroundTraceGets(name *string, reply *interface{}) error {

	err := service.callClientsMethods(*name, "ForegroundTraceGets",-1)
	if err != nil {
		logrus.Errorln(err)
	}
	return err
	//
	//task := Task{MethodName: "TracePuts", Args: name}
	//service.tasks <- task

}


func (service *DirectoryService) GenerateParity(name *string, reply *interface{}) error {
	err := service.callClientsMethods(*name, "GenerateParity",-1)
	if err != nil {
		logrus.Errorln(err)
	}
	return err
	//task := Task{MethodName: "GenerateParity", Args: name}
	//service.tasks <- task

	//return nil
}

func (service *DirectoryService) TraceDget(name *string, reply *interface{}) error {

	err := service.callClientsMethods(*name, "TraceDget",0)
	if err != nil {
		logrus.Errorln(err)
	}
	return err
	//task := Task{MethodName: "TraceDget", Args: name}
	//service.tasks <- task

	//return nil
}

type RecoveryTask struct {
	PGId     int
	Broken   int
	NewBrick uuid.UUID

	BlockId     int
	BlockOffset uint64
	Size        uint64
}

var volumeTasks sync.Map = sync.Map{}

func (service *DirectoryService) PullRecoveryTask(volumeName *string, reply *RecoveryTask) error {
	name := *volumeName
	loaded, ok := volumeTasks.Load(name)
	if !ok {
		return common.ErrVolumeNotExist
	}
	taskChan := loaded.(chan RecoveryTask)
	task,more := <-taskChan
	if !more{
		return common.ErrNoTaskLeft
	} else{
		*reply = task
	}
	return nil
}

func (service *DirectoryService) Recovery(args *RecoveryDispatchArgs, reply *interface{}) error {

	service.waitForInitialization()

	tasks,err := service.getAllTasks(args)
	if err != nil{
		logrus.Errorln(err)
		return err
	}
	volumeTasks.Store(args.VolumeName,tasks)

	err = service.callClientsMethods(args.VolumeName,"Recovery",-1)
	if err != nil {
		logrus.Errorln(err)
	}
	return err

}
func (service *DirectoryService) getAllTasks(args *RecoveryDispatchArgs) (chan RecoveryTask,error) {


	var instructs RecoverReply
	_ = service.RecoverDispatch(args, &instructs)

	loaded, ok := service.Volumes.Load(args.VolumeName)
	if !ok {
		return nil,common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)

	volume.mu.RLock()
	nClients := len(volume.HttpServers)
	myAddr := service.myAddr(volume)
	volume.mu.RUnlock()

	taskChan := make(chan RecoveryTask, nClients*common.RecoveryConcurrentNum)

	dirConn := common.MakeConnection(myAddr)
	pool := MakeVolumeConnectionPool(args.VolumeName, dirConn)

	var wg sync.WaitGroup
	wg.Add(len(instructs))


	taskMap := make(map[int][]RecoveryTask)
	lock := sync.Mutex{}

	for id, ins := range instructs {
		go func(id int,ins MoveInstruct) {
			conn := pool.GetClientConnectionById(rand.Intn(nClients))
			var reply []RecoveryTask
			err := conn.Call("Client.GetRecoveryTasks", &ins, &reply)
			if err != nil {
				logrus.Errorln(err)
			}
			lock.Lock()
			taskMap[id] = reply
			lock.Unlock()
			wg.Done()
		}(id,ins)
	}
	wg.Wait()


	pool.Release()
	dirConn.Close()


	go func(taskMap map[int][]RecoveryTask){
		n := len(instructs)
		for {
			exist := 0
			for i := 0; i < n; i++ {
				tasks := taskMap[i]
				if len(tasks) > 0 {
					taskChan <- tasks[0]
					taskMap[i] = tasks[1:]
					exist ++
				}
			}
			if exist == 0{
				break
			}
		}
		close(taskChan)
	}(taskMap)

	return taskChan,nil
}
