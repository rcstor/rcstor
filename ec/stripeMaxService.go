package ec

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/encoder"
	"rcstor/indexservice"
	"rcstor/storageservice"
	"sync"
	"time"
)

type StripedMaxMSRService struct {
	encoder *encoder.MSREncoder
	RSCoder *encoder.RSEncoder
	pool    *dir.VolumeConnectionPool
	bufPool *common.IOBufferPool

	k          int
	redundancy int
}

func MakeStripedMaxMSRService(pool *dir.VolumeConnectionPool) *StripedMSRService {
	service := &StripedMSRService{}
	service.pool = pool
	service.bufPool = common.MakeIOBufferPool()

	volume := pool.GetVolume()
	parameter := volume.Parameter
	service.encoder = encoder.MakeMSREncoder(parameter.K+parameter.Redundancy, parameter.K, service.bufPool)
	service.k = parameter.K
	service.redundancy = parameter.Redundancy

	return service
}
func (service *StripedMaxMSRService) WriteResponse(w io.Writer, objectID uint64, offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex, broken int) {
	k := service.k
	n := service.k + service.redundancy
	index := objectIndex.StripeMax
	blockSize := index.BlockSize

	if offset+size > index.Size {
		size = index.Size - offset
	}

	readBlock := make(chan common.IOBuffer, 20)
	finished := make(chan bool)

	go func() {
		var transferTime time.Duration
		for buff := range readBlock {
			begin := time.Now()
			n, err := w.Write(buff.Data)
			transferTime += time.Since(begin)
			if n != len(buff.Data) || err != nil {
				logrus.Errorln(err)
			}
			buff.Unref()
		}
		logrus.Infoln("Transfer time consumed on object", objectID, transferTime.String())
		finished <- true
	}()

	chosen := rand.Int()%(n-k) + k

		var wg sync.WaitGroup
		data := make([]common.IOBuffer, n)
		outputs := make([]common.IOBuffer, n-k)
		for i := range data {
			if i != broken && (i < k || (broken != -1 && i == chosen)) {
				wg.Add(1)
				go func(i int) {
					var args stor.GetBatchDataArgs
					args.BlockId = append(args.BlockId, 0)
					args.Offset = append(args.Offset, index.Offset)
					args.Size = append(args.Size, uint32(blockSize))
					conn := service.pool.GetConnectionByBrick(PG.Bricks[i])
					data[i] = getData(&args, conn, service.bufPool)
					wg.Done()
				}(i)
			}
		}

		wg.Wait()
		if broken != -1 && broken<k {
			for j := range outputs {
				outputs[j] = service.bufPool.GetBuffer(int(blockSize))
			}
			service.encoder.Encode(data, outputs)
		}

		off := uint64(0)
			if off < offset+size && offset < off+blockSize {
				for j := 0; j < k; j++ {
					if off < offset+size && offset < off+blockSize {
						st := uint64(0)
						ed := offset + size - off
						if off < offset {
							st = offset - off
						}
						if ed > blockSize {
							ed = blockSize
						}
						output := service.bufPool.GetBuffer(int(ed-st))
						copy(output.Data,data[j].Data[st: ed])

						readBlock <- output
					}
					off += blockSize
				}
			}

		for i:=0;i<n;i++{
			data[i].Unref()
		}


	close(readBlock)
	<-finished

}
func (service *StripedMaxMSRService) GetRecoverTasks(instruct dir.MoveInstruct) []dir.RecoveryTask {
	PG:=instruct.PG

	volume := service.pool.GetVolume()
	pgId := int(PG.PGId)
	if pgId >= len(volume.PGs) {
		return nil
	}

	pg := volume.PGs[pgId]
	var indexes []index.ObjectIndex


		indexBrick := pg.Index[0]

		conn := service.pool.GetConnectionByBrick(indexBrick)

		err := conn.Call("IndexService.GetAllIndex", &pgId, &indexes)
		if err != nil {
			logrus.Errorln("Failed to get index for",pgId,err)
			return nil
		}

	res := make([]dir.RecoveryTask,0)

	for i:=0;i<len(indexes);i++ {
		res = append(res,dir.RecoveryTask{PGId: int(instruct.PG.PGId),Broken: instruct.Broken,NewBrick: instruct.NewBrick,BlockId: 0,BlockOffset: indexes[i].StripeMax.Offset,Size: indexes[i].StripeMax.Size})
	}
	return res
}

func (service *StripedMaxMSRService) GenerateParity(pg dir.PlacementGroup) {
	return
}

func (service *StripedMaxMSRService) Put(objectID uint64, data common.IOBuffer, PG dir.PlacementGroup, objectIndex index.ObjectIndex) {
	n := service.k + service.redundancy
	k := service.k
	index := objectIndex.StripeMax
	blockSize := index.BlockSize


	finalData := make([]common.IOBuffer, n)
	size := uint64(len(data.Data))
	left := size % (blockSize * uint64(k))
	minSize := uint64(256 * k)

	padding := minSize - left%minSize
	finalSize := (size + padding) / uint64(k)

	var wg sync.WaitGroup
	for i := range finalData {
		finalData[i] = service.bufPool.GetBuffer(int(finalSize))
	}
	for i, offset := uint64(0), uint64(0); offset < uint64(len(data.Data)); i++ {
		inputs := make([]common.IOBuffer, n)
		outputs := make([]common.IOBuffer, n-k)
		for j := range outputs {
			outputs[j] = service.bufPool.GetBuffer(int(blockSize))
		}
		for j := range inputs[:k] {
			if offset+blockSize > uint64(len(data.Data)) {
				inputs[j] = service.bufPool.GetBuffer(int(left + padding)/k)
				if offset < uint64(len(data.Data)) {
					copied := copy(inputs[j].Data, data.Data[offset:])
					for l := range inputs[j].Data[copied:] {
						inputs[j].Data[len(data.Data)-int(offset)+l] = 0
					}
				} else {
					for l := range inputs[j].Data {
						inputs[j].Data[l] = 0
					}
				}
				offset += (left + padding)/uint64(k)
			} else {
				inputs[j] = common.IOBuffer{Data: data.Data[offset : offset+blockSize]}
				offset += blockSize
			}

		}
		service.encoder.Encode(inputs, outputs)
		for j := range inputs {
			wg.Add(1)
			go func(j int) {
				copy(finalData[j].Data[i*blockSize:], inputs[j].Data)
				inputs[j].Unref()
				wg.Done()
			}(j)
		}
		wg.Wait()
	}

	for i := range finalData {
		wg.Add(1)
		go func(i int) {
			err := putSlide(finalData[i], 0,uint64(finalSize), index.Offset , 0, PG.PGId, service.pool.GetConnectionByBrick(PG.Bricks[i]))
			if err != nil {
				logrus.Errorln(err)
			}
			finalData[i].Unref()
			wg.Done()
		}(i)
	}
	wg.Wait()

}

func (service *StripedMaxMSRService) repairBlock(pg dir.PlacementGroup, broken int, blockId int16, offset, size uint64) common.IOBuffer {
	var offs []encoder.OffAndSize


	offs = service.encoder.GetRegenerateOffset(broken, offset, size)

	n := service.k + service.redundancy
	var wg sync.WaitGroup
	data := make([]common.IOBuffer, n)

	wg.Add(n - 1)
	for j := 0; j < n; j++ {
		if j != broken {
			go func(j int) {
				start := time.Now()
				var args stor.GetBatchDataArgs
				args.PGId = int(pg.PGId)
				for _, v := range offs {
					args.BlockId = append(args.BlockId, 0)
					args.Offset = append(args.Offset, v.Offset)
					args.Size = append(args.Size, uint32(v.Size))
				}
				conn := service.pool.GetConnectionByBrick(pg.Bricks[j])
				stor.Compress(&args)
				if j == 0{
					logrus.Infoln(args)
				}
				//conn.Call("StorageService.GetBatchData",args,&data[j])
				data[j] = getData(&args,conn,service.bufPool)

				logrus.Debugln("Got data with size", len(data[j].Data), "for brick", j, "cost:", time.Since(start))
				wg.Done()
			}(j)
		}
	}
	wg.Wait()
	start := time.Now()
	data_broken := service.bufPool.GetBuffer(int(size))
	service.encoder.Regenerate(data, data_broken)
	for j := 0; j < n; j++ {
		if j != broken {
			data[j].Unref()
		}
	}
	logrus.Debugln("Regenerate data cost:", time.Since(start))
	return data_broken
}

func (service *StripedMaxMSRService) Recovery() {
	conn := service.pool.GetDirConnection()
	volume := service.pool.GetVolume()

	maxWrites := make(chan bool,common.RecoveryConcurrentNum * 4)
	for i:=0;i<common.RecoveryConcurrentNum * 4;i++ {
		maxWrites<-true
	}

	maxTasks := make(chan bool,common.RecoveryConcurrentNum)
	for i:=0;i<common.RecoveryConcurrentNum ;i++ {
		maxTasks<-true
	}

	for {
		var task dir.RecoveryTask

		err := conn.Call("DirectoryService.PullRecoveryTask", &volume.VolumeName, &task)

		if err != nil && err.Error() == common.ErrNoTaskLeft.Error() {
			break
		} else if err != nil{
			logrus.Errorln(err)
			time.Sleep(time.Second)
			continue
		}

		<-maxTasks
		go func(task dir.RecoveryTask) {
			logrus.Infoln("Handling recovery task",task)
			PGId := task.PGId
			broken := task.Broken
			dest := task.NewBrick

			var args stor.PutDataArgs
			args.BlockId = int16(task.BlockId)
			args.Offset = task.BlockOffset
			args.Len = task.Size

			args.PGId = uint32(PGId)
			data := service.repairBlock(volume.PGs[PGId], broken, int16(task.BlockId), args.Offset, args.Len)
			//Time consuming, but with little concurrency. Should not block the running of repair.
			go func(brick uuid.UUID, args *stor.PutDataArgs, data common.IOBuffer) {
				<-maxWrites
				conn := service.pool.GetConnectionByBrick(brick)

				err := putSlide(data,0,args.Len,args.Offset,args.BlockId,args.PGId,conn)
				if err != nil {
					logrus.Errorln(err)
				}
				data.Unref()

				maxWrites <- true
			}(dest, &args,data)
			logrus.Infoln("Finish recovery task",task)
			maxTasks<-true
		}(task)
	}

	for i:=0;i<common.RecoveryConcurrentNum;i++{
		<-maxTasks
	}
	logrus.Infoln("Begin to wait for writing on disk.")
	for i:=0;i<common.RecoveryConcurrentNum * 4;i++{
		<-maxWrites
	}
}
