package index

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"rcstor/common"
	"strconv"
	"sync"
)

type PGIndexInfo struct {
	IndexMap    map[uint64]*ObjectIndex
	IndexLogTail   uint64
	SpaceUsageInfo	interface{}
}

type IndexService struct {
	PGInfoTable map[uint32]*PGIndexInfo
	mu          sync.RWMutex
	logFdTable  map[uint32]*os.File
	workspace   string
	indexConstructor IndexConstructor
}

func MakeIndexService(workspace string, indexConstructor IndexConstructor) *IndexService {
	service := new(IndexService)
	service.workspace = workspace
	service.indexConstructor = indexConstructor
	service.PGInfoTable = make(map[uint32]*PGIndexInfo)
	service.logFdTable = make(map[uint32]*os.File)
	for _,pgId := range service.getAllPGIds() {
		dirPath := service.logDirPath(pgId)
		if _, err := os.Stat(dirPath); err == nil || os.IsExist(err) {
			pginfo := service.getPGInfo(pgId)
			Recovery(dirPath, pginfo, service)
		}
	}
	return service
}
func (service *IndexService) getAllPGIds() []uint32{
	pgs,err := ioutil.ReadDir(filepath.Join(service.workspace,"index"))
	if err != nil && !os.IsNotExist(err){
		log.Errorln("Error occured reading index:",err)
	}
	res := make([]uint32,0)
	for _,pg:= range pgs {
		if len(pg.Name())<=2 || !pg.IsDir() {
			continue
		}
		pgid,err := strconv.Atoi(pg.Name()[3:])
		if err != nil || pgid<0{
			continue
		}
		res = append(res,uint32(pgid))
	}
	return res
}

func (service *IndexService) GetAllIndex(PGId *uint32, reply *[]ObjectIndex) error {
	for _, v := range service.PGInfoTable[*PGId].IndexMap {
		*reply = append(*reply, *v)
	}
	return nil
}

func (service *IndexService) GetIndex(args *GetIndexArgs, reply *ObjectIndex) error {
	service.mu.RLock()
	defer service.mu.RUnlock()
	pginfo, ok := service.PGInfoTable[args.PGId]
	if !ok {
		log.Errorln("Invalid PGid",args.PGId)
		log.Errorln(service.PGInfoTable)
		return common.ErrObjectNotFound
	}
	index,exist := pginfo.IndexMap[args.ObjectId]
	if !exist{
		log.Errorln("Get index failed",pginfo.IndexMap,pginfo.IndexLogTail)
		return common.ErrObjectNotFound
	}
	*reply = *index
	return nil
}

func (service *IndexService) PutIndex(args *PutIndexArgs, reply *ObjectIndex) error {
	service.mu.Lock()
	defer service.mu.Unlock()
	pginfo := service.getPGInfo(args.PGId)

	if index, ok := pginfo.IndexMap[args.ObjectID]; ok {
		//Update the object
		if index.GetSize() == args.Size {
			*reply = *index
			return nil
		} else {
			return errors.New("same object id but different size")
		}
	}


	index := service.indexConstructor.Construct(args,&pginfo.SpaceUsageInfo)
	//service.indexConstructor.UpdateSpace(index,&pginfo.SpaceUsageInfo)

	pginfo.IndexMap[args.ObjectID] = index
	pginfo.IndexLogTail++

	AddLog(args, service.logDirPath(args.PGId), pginfo.IndexLogTail, pginfo, service.logFdTable)

	*reply = *index
	return nil
}


func (service *IndexService) Drop(args *int, reply *interface{}) error {
	service.PGInfoTable = make(map[uint32]*PGIndexInfo)
	service.logFdTable = make(map[uint32]*os.File)

	DropFolder(filepath.Join(service.workspace,"index"))

	return nil
}

func (service *IndexService) logDirPath(PGId uint32) string {
	return filepath.Join(service.workspace, "index", fmt.Sprintf("PG-%d", PGId))
}

func (service *IndexService) getPGInfo(PGId uint32) *PGIndexInfo {
	pginfo, ok := service.PGInfoTable[PGId]
	if !ok {
		pginfo = &PGIndexInfo{
			IndexMap:    make(map[uint64]*ObjectIndex),
			IndexLogTail:   0,
			SpaceUsageInfo: nil,
		}
		service.PGInfoTable[PGId] = pginfo
	}
	return pginfo
}
