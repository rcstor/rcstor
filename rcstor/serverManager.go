package main

import (
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"path/filepath"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/indexservice"
	stor "rcstor/storageservice"
	"time"
)

func startStorageServerLocal(path string, constructor index.IndexConstructor, storage stor.StorageServiceInterface) uint16 {
	addr := "0.0.0.0:0"
	s, l, _ := common.StartRPCServer(addr)
	indexService := index.MakeIndexService(path,constructor)

	s.RegisterName("IndexService", indexService)
	s.RegisterName("StorageService", storage)

	listener, err := net.ListenTCP("tcp", l)
	if err != nil {
		log.Fatalln(err)
	}

	go common.ServeRPCServer(s, listener,storage.HandleDirect)
	return uint16(listener.Addr().(*net.TCPAddr).Port)
}

func StartServer(volumeName,workspace,dirAddr string) {
	path := filepath.Join(workspace,volumeName)

	storage := stor.MakeStorageService(path)

	var args dir.BrickRegisterArgs
	args.VolumeName = volumeName

	var para dir.VolumeParameter

	var err error
	conn := common.MakeConnection(dirAddr)
	err = conn.Call("DirectoryService.BrickRegister",&args,&para)
	if err != nil {
		log.Fatalln(err)
	}

	var port uint16

	//FIXME: port may be used by other processes.
	switch(para.Layout) {
	case common.Contiguous:
		port = startStorageServerLocal(path,&index.ContiguousIndexConstructor{K: para.K}, storage)
	case common.Geometric,common.Hitchhiker:
		partitioner := &index.GeometricPartitioner{Base: para.GeometricBase,MinBlock: para.MinBlockSize,MaxBlock: para.MaxBlockSize}
		port = startStorageServerLocal(path,&index.GeometricIndexConstructor{K: para.K,Partitioner:*partitioner}, storage)
	case common.Stripe,common.LRC,common.RS:
		port = startStorageServerLocal(path,&index.StripedIndexConstructor{K: para.K,StripSize: para.BlockSize}, storage)
	case common.StripeMax:
		port = startStorageServerLocal(path,&index.StripedMaxIndexConstructor{K: para.K,Alignment: para.Alignment}, storage)
	}

	args.Dir = workspace
	args.BrickID = storage.UUID
	args.Port = port
	args.PID = os.Getpid()
	storage.IsSSD = para.IsSSD


	go func() {
		for {
			err = conn.Call("DirectoryService.BrickRegister",&args,nil)
			if err != nil {
				log.Errorln(err)
			}
			time.Sleep(common.HeartBeatInterval)
		}
	}()

}