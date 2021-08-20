package dir

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"rcstor/common"
	"strconv"
	"testing"
)

func TestRecoveryDispatch(t *testing.T) {
	volume :=&Volume{}
	volume.Parameter.NumberPG = 200
	volume.Parameter.K = 10
	volume.Parameter.Redundancy = 4
	nServer := 16
	volume.Servers = make([]ServerInfo,nServer)
	volume.Bricks = make(map[uuid.UUID]*Brick)
	for i:=0;i<nServer;i++{
		volume.Servers[i].Dir = make([]string,6)
		volume.Servers[i].IP = strconv.Itoa(i)
		volume.Servers[i].BrickId = make([]uuid.UUID,6)
		for j:=0;j<6;j++ {
			volume.Servers[i].BrickId[j] = uuid.NewV1()
			volume.Bricks[volume.Servers[i].BrickId[j]] = &Brick{IP: volume.Servers[i].IP,Dir: strconv.Itoa(j)}
		}
	}
	volume.assignPGs()

	args := &RecoveryDispatchArgs{BrokenBrick: volume.Servers[0].BrickId[0]}
	var reply RecoverReply
	volume.dispatch(args,&reply)
	logrus.Println(len(reply))
}


func TestVolume(t *testing.T) {
	service :=  StartDirectoryService("./db",30100)
	servers := make([]ServerInfo,1)
	servers[0].IP="localhost"
	servers[0].Dir = make([]string,14)
	for i:=0;i<14;i++ {
		servers[0].Dir[i] = "/disks/disk"+strconv.Itoa(i)
	}


	var para VolumeParameter
	para.NumberPG = 10
	para.Layout = common.Contiguous
	para.K = 10
	para.Redundancy = 4
	para.BlockSize = (1<<20)

	args := CreateVolumeArgs{VolumeName: "Contiguous-1M", Servers: servers, Parameter: para}
	err := service.CreateVolume(&args,nil)
	logrus.Println(service.Volumes.Load("Contiguous-1M"))
	if err != nil {
		logrus.Errorln(err)
	}
	err = service.StartVolume(&args.VolumeName,nil)
	if err != nil {
		logrus.Errorln(err)
	}

	err = service.StopVolume(&args.VolumeName,nil)
	if err != nil {
		logrus.Errorln(err)
	}

}
