package ec

import (
	uuid "github.com/satori/go.uuid"

	//"encoding/gob"
	log "github.com/sirupsen/logrus"
	"io"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/encoder"
	"rcstor/indexservice"
	"rcstor/storageservice"

	"sync"
	"time"
)

type ContiguousService struct {
	encoder *encoder.MSREncoder
	pool    *dir.VolumeConnectionPool
	bufPool *common.IOBufferPool

	blockSize  uint64
	k          int
	redundancy int
}

func MakeContiguousService(pool *dir.VolumeConnectionPool) *ContiguousService {
	service := &ContiguousService{}
	service.pool = pool
	service.bufPool = common.MakeIOBufferPool()

	volume := pool.GetVolume()
	parameter := volume.Parameter
	service.encoder = encoder.MakeMSREncoder(parameter.K+parameter.Redundancy, parameter.K, service.bufPool)
	service.blockSize = parameter.BlockSize
	service.k = parameter.K
	service.redundancy = parameter.Redundancy
	return service
}

func (service *ContiguousService) WriteResponse(w io.Writer, objectID uint64, offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex, broken int) {
	index := objectIndex.Contiguous

	pipeBlock := uint64(1 << 20)

	readBlock := make(chan common.IOBuffer, 8)
	finished := make(chan bool)

	go func() {
		for buff := range readBlock {
			n, err := w.Write(buff.Data)
			if n != len(buff.Data) || err != nil {
				log.Errorln(err)
			}
			//FIXME:need to be cleaned
			buff.Unref()
		}
		finished <- true
	}()

	if int(index.IndexInPG) == broken {

		start_block := (index.Offset + offset) / service.blockSize
		end_block := (index.Offset + offset + size - 1) / service.blockSize

		for i := start_block; i <= end_block; i++ {
			st := uint64(0)
			if i*service.blockSize < offset+index.Offset {
				st = offset + index.Offset - i*service.blockSize
			}
			ed := uint64(service.blockSize)
			if (i+1)*service.blockSize > index.Offset+offset+size {
				ed = offset + size + index.Offset - i*service.blockSize
			}
			block := service.RegenerateBlock(i*service.blockSize, service.blockSize,int(index.IndexInPG),PG)
			readBlock <- block.Slice(st, ed)
		}
	} else {
		for off := offset; off < offset+size; off += pipeBlock {
			if off+pipeBlock > offset+size {
				pipeBlock = offset + size - off
			}
			readBlock <- service.Get(objectID, off, pipeBlock, PG, objectIndex)
		}
	}
	close(readBlock)
	<-finished
}

func (service *ContiguousService) RegenerateBlock(offset uint64, size uint64, indexInPG int, PG dir.PlacementGroup) common.IOBuffer {
	n := service.k + service.redundancy
	log.Debugln("size:", size, "IndexInPG:", indexInPG, "PGID:",PG.PGId)
	var wg sync.WaitGroup
	data := make([]common.IOBuffer, n)
	offs := service.encoder.GetRegenerateOffset(indexInPG, offset, size)


	wg.Add(n - 1)
	for j := 0; j < n; j++ {
		if j != indexInPG {
			go func(j int) {
				start := time.Now()
				var args stor.GetBatchDataArgs
				args.PGId = int(PG.PGId)
				for _, v := range offs {
					args.BlockId = append(args.BlockId, 0)
					args.Offset = append(args.Offset, v.Offset)
					args.Size = append(args.Size, uint32(v.Size))
				}
				conn := service.pool.GetConnectionByBrick(PG.Bricks[j])
				stor.Compress(&args)
				//conn.Call("StorageService.GetBatchData",args,&data[j])
				data[j] = getData(&args,conn,service.bufPool)

				log.Debugln("Got data with size", len(data[j].Data), "for brick", j, "cost:", time.Since(start))
				wg.Done()
			}(j)
		}
	}
	wg.Wait()
	start := time.Now()
	data_broken := service.bufPool.GetBuffer(int(size))
	service.encoder.Regenerate(data, data_broken)
	for j := 0; j < n; j++ {
		if j != indexInPG {
			data[j].Unref()
		}
	}
	log.Debugln("Regenerate data cost:", time.Since(start))
	return data_broken
}

func (service *ContiguousService) Get(objectID uint64, offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex) common.IOBuffer {
	index := objectIndex.Contiguous
	conn := service.pool.GetConnectionByBrick(PG.Bricks[index.IndexInPG])
	var args stor.GetDataArgs
	args.BlockId = -1
	args.PGId = PG.PGId
	args.Offset = index.Offset + offset
	args.Size = uint32(common.Min(size, index.Size-offset))
	var reply common.IOBuffer
	conn.Call("StorageService.GetData", &args, &reply)

	return reply
}
func (service *ContiguousService) Put(objectID uint64, data common.IOBuffer, PG dir.PlacementGroup, objectIndex index.ObjectIndex) {
	index := objectIndex.Contiguous
	conn := service.pool.GetConnectionByBrick(PG.Bricks[index.IndexInPG])
	//log.Println("Begin to put", objectID)
	putBlock := uint64(4 << 20)
	for offset := uint64(0); offset < index.Size; offset += putBlock {
		if index.Size < offset+putBlock {
			putBlock = index.Size - offset
		}
		err := putSlide(data, offset, putBlock, index.Offset+offset, 0, PG.PGId, conn)
		if err != nil {
			log.Errorln(err)
		}
	}
}

func (service *ContiguousService) getMaxSize(pg dir.PlacementGroup) uint64 {
	k := service.k
	sizes := make([]uint64, k)
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			conn := service.pool.GetConnectionByBrick(pg.Bricks[i])
			var args stor.GetBlockSizeArgs
			args.BlockId = -1
			args.PGId = pg.PGId
			conn.Call("StorageService.GetBlockSize", &args, &sizes[i])
			wg.Done()
		}(i)
	}
	wg.Wait()
	maxSize := uint64(0)
	for i := range sizes {
		if sizes[i] > maxSize {
			maxSize = sizes[i]
		}
	}
	return maxSize
}

func (service *ContiguousService) RepairBlocks(pg dir.PlacementGroup, offset uint64, broken int, shuffled bool) []common.IOBuffer {

	n := service.k + service.redundancy
	k := service.k

	var wg sync.WaitGroup

	data := make([]common.IOBuffer, n)
	outputs := make([]common.IOBuffer, n-k)

	wg.Add(k)

	order := make([]int, n)
	if shuffled {
		order = common.OrderShuffle(n)
	} else {
		for i := 0; i < n; i++ {
			order[i] = i
		}
	}

	asked := 0
	for i := 0; i < n && asked < k; i++ {
		if order[i] != broken {
			asked++
			go func(brickNum int) {
				conn := service.pool.GetConnectionByBrick(pg.Bricks[brickNum])
				var args stor.GetDataArgs
				args.BlockId = -1
				args.PGId = pg.PGId
				args.Offset = offset
				args.Size = uint32(service.blockSize)
				conn.Call("StorageService.GetData", &args, &data[brickNum])
				wg.Done()
			}(order[i])
		}
	}

	wg.Wait()

	for i := range outputs {
		outputs[i] = service.bufPool.GetBuffer(int(service.blockSize))
	}
	service.encoder.Encode(data, outputs)

	return data
}

func (service *ContiguousService) GenerateParity(pg dir.PlacementGroup) {

	n := service.k + service.redundancy
	k := service.k
	maxSize := service.getMaxSize(pg)

	var wg sync.WaitGroup

	for offset := uint64(0); offset < maxSize; offset += service.blockSize {
		data := service.RepairBlocks(pg, offset, -1, false)
		for i := range data[:k] {
			data[i].Unref()
		}
		wg.Add(service.redundancy)
		for i := k; i < n; i++ {
			go func(i int) {
				conn := service.pool.GetConnectionByBrick(pg.Bricks[i])

				err := putSlide(data[i],0,uint64(len(data[i].Data)),offset,0,pg.PGId,conn)
				if err != nil {
					log.Errorln(err)
				}
				data[i].Unref()
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
}

//for task <- tasks : {data=repairTask(task);go put data}
func (service *ContiguousService) Recovery() {

	conn := service.pool.GetDirConnection()
	volume := service.pool.GetVolume()

	maxWrites := make(chan bool,common.RecoveryConcurrentNum * 4)
	for i:=0;i<common.RecoveryConcurrentNum * 4;i++ {
		maxWrites<-true
	}

	maxTasks := make(chan bool,common.RecoveryConcurrentNum)
	for i:=0;i<common.RecoveryConcurrentNum;i++ {
		maxTasks<-true
	}

	for {
		var task dir.RecoveryTask

		err := conn.Call("DirectoryService.PullRecoveryTask", &volume.VolumeName, &task)

		if err != nil && err.Error() == common.ErrNoTaskLeft.Error() {
			break
		} else if err != nil{
			log.Errorln(err)
			time.Sleep(time.Second)
			continue
		}

		<-maxTasks
		go func(task dir.RecoveryTask) {
			log.Infoln("Handling recovery task",task)
			PGId := task.PGId
			broken := task.Broken
			dest := task.NewBrick

			var args stor.PutDataArgs
			args.BlockId = int16(task.BlockId)
			args.Offset = task.BlockOffset
			args.Len = service.blockSize

			args.PGId = uint32(PGId)
			data := service.repairBlock(volume.PGs[PGId], broken, int16(task.BlockId), args.Offset, service.blockSize)
			//Time consuming, but with little concurrency. Should not block the running of repair.
			go func(brick uuid.UUID, args *stor.PutDataArgs, data common.IOBuffer) {
				<-maxWrites
				conn := service.pool.GetConnectionByBrick(brick)

				err := putSlide(data,0,args.Len,args.Offset,args.BlockId,args.PGId,conn)
				if err != nil {
					log.Errorln(err)
				}
				data.Unref()

				maxWrites <- true
			}(dest, &args,data)
			log.Infoln("Finish recovery task",task)
			maxTasks<-true
		}(task)
	}

	for i:=0;i<common.RecoveryConcurrentNum;i++{
		<-maxTasks
	}
	log.Infoln("Begin to wait for writing on disk.")
	for i:=0;i<common.RecoveryConcurrentNum * 4;i++{
		<-maxWrites
	}

}

func (service *ContiguousService) GetRecoverTasks(instruct dir.MoveInstruct) []dir.RecoveryTask {
	PG:=instruct.PG
	broken := instruct.Broken
	toRecover := uint64(0)
	conn := service.pool.GetConnectionByBrick(PG.Bricks[broken])
	var args stor.GetBlockSizeArgs
	args.BlockId = 0
	args.PGId = PG.PGId
	conn.Call("StorageService.GetBlockSize", &args, &toRecover)
	res := make([]dir.RecoveryTask,0)
	for offset := uint64(0); offset < toRecover; offset += service.blockSize {
		res = append(res,dir.RecoveryTask{PGId: int(instruct.PG.PGId),Broken: instruct.Broken,NewBrick: instruct.NewBrick,BlockId: 0,BlockOffset: offset,Size: service.blockSize})
	}
	return res

}
func (service *ContiguousService) repairBlock(pg dir.PlacementGroup, broken int, blockId int16, offset, size uint64) common.IOBuffer {
	offs := service.encoder.GetRegenerateOffset(broken, offset, size)
	msrRegenerate := true

	var args stor.GetBatchDataArgs
	for _, v := range offs {
		args.BlockId = append(args.BlockId, blockId)
		args.Offset = append(args.Offset, v.Offset)
		args.Size = append(args.Size, uint32(v.Size))
	}
	stor.Compress(&args)
	if args.Size[0] <= (512 << 10) {
		msrRegenerate = false
	}

	if msrRegenerate {
		return service.RegenerateBlock(offset, size, broken, pg)
	} else {
		data := service.RepairBlocks(pg, offset, broken, true)
		for i := range data {
			if i != broken {
				data[i].Unref()
			}
		}
		return data[broken]
	}
}