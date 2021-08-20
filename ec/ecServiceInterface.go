package ec

import (
	"encoding/binary"
	"encoding/gob"
	log "github.com/sirupsen/logrus"
	"io"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/indexservice"
	"rcstor/storageservice"
	"time"
)

type ECServiceInterface interface {
	Put(objectID uint64, data common.IOBuffer, PG dir.PlacementGroup, objectIndex index.ObjectIndex)

	GenerateParity(pg dir.PlacementGroup)

	Recovery()

	GetRecoverTasks(instruct dir.MoveInstruct) []dir.RecoveryTask

	WriteResponse(w io.Writer, objectID uint64, offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex, broken int)
}

func MakeECService(pool *dir.VolumeConnectionPool) ECServiceInterface {
	volume := pool.GetVolume()
	switch volume.Parameter.Layout {
	case common.Contiguous:
		return MakeContiguousService(pool)
	case common.Geometric:
		return MakeGeometricService(pool)
	case common.Stripe:
		return MakeStripedMSRService(pool)
	case common.StripeMax:
		return MakeStripedMaxMSRService(pool)
	case common.RS:
		return MakeStripedRSService(pool)
	case common.LRC:
		return MakeStripedLRCService(pool)
	case common.Hitchhiker:
		return MakeHitchhikerService(pool)
	}
	return nil
}

func getDataBySizes(args *stor.GetBatchDataArgs, conn *common.Connection, bufPool *common.IOBufferPool,sizes []uint32,readNext chan bool) chan common.IOBuffer {
	if readNext != nil{
		args.NetSizes = sizes
	} else {
		args.NetSizes = nil
	}
	ret := make(chan common.IOBuffer)
	go func() {
		bufferRead := 0
		for {
			rwc := conn.GetDirectConn()

			n, err := rwc.Write([]byte{stor.GetBatch})
			if n != 1 || err != nil {
				rwc.Close()
				conn.DropDirectConn()
				time.Sleep(time.Second)
				continue
			}

			err = gob.NewEncoder(rwc).Encode(args)
			if err != nil {
				log.Errorln(err)
				rwc.Close()
				conn.DropDirectConn()
				time.Sleep(time.Second)
				continue
			}

			var i int
			for i=0;i<bufferRead;i++{
				buffer := bufPool.GetBuffer(int(sizes[i]))
				n, err = io.ReadFull(rwc, buffer.Data)
				if n != len(buffer.Data) || err != nil {
					log.Errorln(n, err)
					rwc.Close()
					buffer.Unref()
					conn.DropDirectConn()
					time.Sleep(time.Second)
					break
				}
				buffer.Unref()
			}
			if i!=bufferRead{
				continue
			}

			for ;bufferRead<len(sizes); {
				//Barrier to synchronize read
				if readNext != nil {
					<-readNext
					n, err := rwc.Write([]byte{stor.GetNext})

					if n != 1 || err != nil {
						rwc.Close()
						conn.DropDirectConn()
						time.Sleep(time.Second)
						break
					}
				}
				buffer := bufPool.GetBuffer(int(sizes[bufferRead]))
				n, err = io.ReadFull(rwc, buffer.Data)
				if n != len(buffer.Data) || err != nil {
					log.Errorln(n, err)
					rwc.Close()
					buffer.Unref()
					conn.DropDirectConn()
					time.Sleep(time.Second)
					break
				}
				ret <- buffer
				bufferRead ++
			}

			if bufferRead != len(sizes) {
				continue
			}

			conn.ReturnDirect(rwc)
			close(ret)
			return
		}
	}()
	return ret
}

func getData(args *stor.GetBatchDataArgs, conn *common.Connection, bufPool *common.IOBufferPool) common.IOBuffer {

	total := uint32(0)
	for _, s := range args.Size {
		total += s
	}
	sizes := []uint32{total}
	readNext := make(chan bool,1)

	ch := getDataBySizes(args,conn,bufPool,sizes,readNext)
	readNext<-true

	return <-ch
}

func putSlide(data common.IOBuffer, offset, size uint64, offsetInBlock uint64, blockId int16, PGId uint32, conn *common.Connection) error {
	var args stor.PutDataArgs
	args.PGId = PGId
	args.BlockId = blockId
	args.Offset = offsetInBlock
	args.Len = size

	for {
		rwc := conn.GetDirectConn()
		n, err := rwc.Write([]byte{stor.Put})
		if n != 1 || err != nil {
			log.Errorln("Failed to write put protocol code ", err)
			rwc.Close()
			conn.DropDirectConn()
			time.Sleep(time.Second)
			continue
		}

		var arg [22]byte
		binary.LittleEndian.PutUint16(arg[:], uint16(blockId))
		binary.LittleEndian.PutUint64(arg[2:], args.Offset)
		binary.LittleEndian.PutUint64(arg[10:], size)
		binary.LittleEndian.PutUint32(arg[18:], PGId)
		n, err = rwc.Write(arg[:])

		if n != 22 || err != nil {
			log.Errorln("Failed to encode put args.", err)
			rwc.Close()
			conn.DropDirectConn()
			time.Sleep(time.Second)
			continue
		}

		n, err = rwc.Write(data.Data[offset : size+offset])
		if n != int(size) || err != nil {
			log.Errorln("Failed to write put data.", err, "Written:", n, "Size:", size)
			rwc.Close()
			conn.DropDirectConn()
			time.Sleep(time.Second)
			continue
		}

		var ret [1]byte
		n, err = rwc.Read(ret[:])
		if n != 1 || err != nil || ret[0] != stor.WriteFinished {
			log.Errorln("Failed to obtain result.", err, ret[0])
			rwc.Close()
			conn.DropDirectConn()
			time.Sleep(time.Second)
			continue
		}

		conn.ReturnDirect(rwc)
		return nil
	}
}
