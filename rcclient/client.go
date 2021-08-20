package rcclient

import (
	"github.com/sirupsen/logrus"
	"io"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/ec"
	"rcstor/indexservice"
)

type Client struct {
	DirConn   *common.Connection

	Pool      *dir.VolumeConnectionPool
	EC        ec.ECServiceInterface
	IobufPool *common.IOBufferPool

	ClientId  int
	NumClients int

	LocalHttpPort uint16
}

func (client *Client) GetPGID(objectID uint64) int {

	var PGId int

	volume := client.Pool.GetVolume()
	//k := client.volume.Parameter.K
	nPG := volume.Parameter.NumberPG

	PGId = int(objectID % uint64(nPG))

	return PGId
}

func (client *Client) GetIndex(objectID uint64) (index.ObjectIndex, dir.PlacementGroup) {
	PGId := client.GetPGID(objectID)
	volume := client.Pool.GetVolume()

	pg := volume.PGs[PGId]
	var objectIndex index.ObjectIndex

	for indexId := 0; indexId < common.REPLICATION; indexId++ {
		indexBrick := pg.Index[indexId]

		conn := client.Pool.GetConnectionByBrick(indexBrick)
		var args index.GetIndexArgs
		args.ObjectId = objectID
		args.PGId = uint32(PGId)
		err := conn.Call("IndexService.GetIndex", &args, &objectIndex)
		if err != nil {
			logrus.Errorln("Failed to get index for",objectID,err)
		} else {
			return objectIndex,pg
		}
	}
	logrus.Fatal("Failed to get index")
	return objectIndex,pg
}


func (client *Client) PutIndex(objectID uint64, size uint64) (index.ObjectIndex, dir.PlacementGroup, error) {
	PGId := client.GetPGID(objectID)
	volume := client.Pool.GetVolume()

	pg := volume.PGs[PGId]
	var objectIndex index.ObjectIndex

	for indexId := 0; indexId < common.REPLICATION; indexId++ {
		indexBrick := pg.Index[indexId]
		//logrus.Infoln(client.volume)

		conn := client.Pool.GetConnectionByBrick(indexBrick)
		var args index.PutIndexArgs
		args.ObjectID = objectID
		args.Size = size
		args.PGId = uint32(PGId)
		err := conn.Call("IndexService.PutIndex", &args, &objectIndex)
		if err != nil && err.Error() == index.ErrExist.Error() {
			logrus.Errorln(err)
			return objectIndex,pg,err
		} else if err != nil {
			logrus.Errorln("Failed to put index",err)
			return objectIndex,pg,err
		}
	}
	return objectIndex,pg,nil
}

func (client *Client) Put(objectId uint64, data common.IOBuffer) {
	size := len(data.Data)

	putIndex, PG, err := client.PutIndex(objectId, uint64(size))
	if err==nil {
		client.EC.Put(objectId, data, PG, putIndex)
	} else {
		logrus.Errorln(err)
	}
}

func (client *Client) Get(w io.Writer, objectId uint64,size uint64,offset uint64)  {
	objectIndex, pg := client.GetIndex(objectId)

	if offset >= objectIndex.GetSize() {
		return
	}
	if offset + size >= objectIndex.GetSize() {
		size = objectIndex.GetSize() - offset
	}

	client.EC.WriteResponse(RateWriter(w,common.MaxClientBandwidth),objectId, offset, size, pg, objectIndex,-1)
}

func (client *Client) GeneratePGParity(pgID int) {
	logrus.Println("Begin to generate parity of PG",pgID)
	volume := client.Pool.GetVolume()
	client.EC.GenerateParity(volume.PGs[pgID])
}

func (client *Client) DGet(w io.Writer, objectId uint64,size uint64,offset uint64)  {
	objectIndex, pg := client.GetIndex(objectId)
	broken := 0

	if offset >= objectIndex.GetSize() {
		return
	}
	if offset + size >= objectIndex.GetSize() {
		size = objectIndex.GetSize() - offset
	}

	if objectIndex.Contiguous != nil {
		broken = int(objectIndex.Contiguous.IndexInPG)
	} else if objectIndex.Geometric != nil {
		broken = int(objectIndex.Geometric.Blocks[0].IndexInPG)
	}
	//client.EC.WriteResponse(w, objectId, offset, size, pg, objectIndex, broken)
	client.EC.WriteResponse(RateWriter(w,common.MaxClientBandwidth), objectId, offset, size, pg, objectIndex, broken)
}


func MakeClient(directoryAddr string, volumeName string) *Client {
	dirConn:= common.MakeConnection(directoryAddr)
	return MakeClientWithConn(dirConn,volumeName)
}

func MakeClientWithConn(dirConn *common.Connection, volumeName string) *Client {
	result := &Client{}

	result.DirConn = dirConn
	result.IobufPool = common.MakeIOBufferPool()

	result.Pool = dir.MakeVolumeConnectionPool(volumeName,dirConn)
	result.EC = ec.MakeECService(result.Pool)

	return result
}