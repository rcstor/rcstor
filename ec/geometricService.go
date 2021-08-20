package ec

import (
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"io"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/encoder"
	"rcstor/indexservice"
	"rcstor/storageservice"

	"sync"
	"time"
)

type GeometricService struct {
	minBlockEncoder *encoder.RSEncoder
	encoder         *encoder.MSREncoder

	pool    *dir.VolumeConnectionPool
	bufPool *common.IOBufferPool

	partitioner *index.GeometricPartitioner

	k          int
	redundancy int

	timestat  common.TimeStats
}

func MakeGeometricService(pool *dir.VolumeConnectionPool) *GeometricService {
	service := &GeometricService{}

	volume := pool.GetVolume()

	service.k = volume.Parameter.K
	service.redundancy = volume.Parameter.Redundancy

	service.minBlockEncoder = encoder.MakeRSEncoder(service.redundancy+service.k, service.k)
	service.pool = pool
	service.bufPool = common.MakeIOBufferPool()
	service.encoder = encoder.MakeMSREncoder(service.k+service.redundancy, service.k, service.bufPool)

	service.partitioner = &index.GeometricPartitioner{MinBlock: volume.Parameter.MinBlockSize, MaxBlock: volume.Parameter.MaxBlockSize, Base: volume.Parameter.GeometricBase}

	return service
}

func (service *GeometricService) WriteResponse(w io.Writer, objectID uint64, offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex, broken int) {

	readBlock := make(chan common.IOBuffer, 20)
	finished := make(chan bool)
	var transferTime time.Duration
	go func() {

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

	var blocks chan common.IOBuffer
	if broken == int(objectIndex.Geometric.Blocks[0].IndexInPG) {
		blocks = service.RegenerateObject(PG, objectIndex.Geometric, offset, size, broken)
	} else {
		blocks = service.Get(offset, size, PG, objectIndex)
	}
	var repairTime time.Duration
	for {
		begin := time.Now()
		block, proceeding := <-blocks
		repairTime += time.Since(begin)
		if !proceeding {
			break
		}
		readBlock <- block
	}
	close(readBlock)
	logrus.Infoln("Repair time consumed on object", objectID, repairTime.String())

	<-finished

	service.timestat.AddTime(repairTime,transferTime)
	rep,trans := service.timestat.Average()
	logrus.Printf("Avg repair time:%d ns, avg transfer time:%d ns.\n",rep,trans)
}

func (service *GeometricService) RepairBlocks(blockId int, offset uint64, size uint64, pg dir.PlacementGroup, broken int, shuffled bool) []common.IOBuffer {


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
				var args stor.GetBatchDataArgs
				args.BlockId = append(args.BlockId, int16(blockId))
				args.PGId = int(pg.PGId)
				args.Offset = append(args.Offset, offset)
				args.Size = append(args.Size, uint32(size))

				data[brickNum] = getData(&args, conn, service.bufPool)
				wg.Done()
			}(order[i])
		}
	}

	wg.Wait()

	for i := range outputs {
		outputs[i] = service.bufPool.GetBuffer(int(size))
	}

	if blockId != 0 {
		service.encoder.Encode(data, outputs)
	} else {
		service.minBlockEncoder.Encode(data, outputs)
	}
	return data
}

func (service *GeometricService) RegenerateObject(PG dir.PlacementGroup, geoIndex *index.GeometricIndex, offset, size uint64, broken int) chan common.IOBuffer {
	var args stor.GetBatchDataArgs
	args.PGId = int(PG.PGId)

	ret := make(chan common.IOBuffer, 32)

	go func() {
		currentOff := uint64(0)

		sizes := make([]uint32, 0)
		blocks := make([]uint64, 0)
		for _, block := range geoIndex.Blocks {
			if block.BlockID == 0 {
				if block.Size == 0 {
					continue
				}
				blocks := service.RepairBlocks(0, block.OffsetInBucket, block.Size, PG, broken, true)
				for i := 0; i < len(blocks); i++ {
					if i != broken {
						blocks[i].Unref()
					}
				}
				start := offset
				end := size + offset
				if end > uint64(block.Size) {
					end = block.Size
				}
				ret <- blocks[broken].Slice(start, end)
				currentOff = end
				continue
			}

			if currentOff+block.Size >= offset && currentOff < offset+size {
				offs := service.encoder.GetRegenerateOffset(broken, block.OffsetInBucket, block.Size)
				total := uint64(0)
				for _, off := range offs {
					args.Offset = append(args.Offset, off.Offset)
					args.Size = append(args.Size, uint32(off.Size))
					args.BlockId = append(args.BlockId, block.BlockID)
					total += off.Size
				}
				sizes = append(sizes, uint32(total))
				blocks = append(blocks, block.Size)
			}
			currentOff += block.Size
		}
		stor.Compress(&args)

		currentOff = 0

		if len(args.BlockId) <= 0 {
			close(ret)
			return
		}

		n := service.k + service.redundancy
		chans := make([]chan common.IOBuffer, n)
		readNext := make([]chan bool, n)
		for i := 0; i < n; i++ {
			if i != broken {
				readNext[i] = make(chan bool, 1)
				chans[i] = getDataBySizes(&args, service.pool.GetConnectionByBrick(PG.Bricks[i]), service.bufPool, sizes, readNext[i])
			}
		}

		dataChan := make(chan []common.IOBuffer, 4)

		go func() {
			i := 0
			for data := range dataChan {
				dataBroken := service.bufPool.GetBuffer(int(blocks[i]))
				service.encoder.Regenerate(data, dataBroken)
				for j := 0; j < n; j++ {
					if j != broken {
						data[j].Unref()
					}
				}
				start := uint64(0)
				if offset > currentOff {
					start = offset
				}
				end := blocks[i]
				if currentOff+blocks[i] > offset+size {
					end = offset + size - currentOff
				}
				ret <- dataBroken.Slice(start, end)
				currentOff += end
				i++
			}
			close(ret)
		}()

		for i := 0; i < len(sizes); i++ {
			data := make([]common.IOBuffer, n)
			start := time.Now()
			for j := 0; j < n; j++ {
				if j != broken {
					readNext[j] <- true
					data[j] = <-chans[j]
					logrus.Debugln("Got data with size", sizes[i], "for brick", j, "cost:", time.Since(start))
					brick := service.pool.GetVolume().Bricks[PG.Bricks[j]]
					logrus.Debugln(brick.IP, brick.Dir)
				}
			}
			dataChan <- data
		}
		close(dataChan)
	}()

	return ret
}

func (service *GeometricService) RegenerateBlock(blockId int, offset uint64, size uint64, broken int, PG dir.PlacementGroup) common.IOBuffer {
	n := service.k + service.redundancy
	logrus.Warnln("PGId:", PG.PGId, "blockId:", blockId, "offset:", offset, "size:", size, " IndexInPG:", broken)

	if blockId == 0 {
		blocks := service.RepairBlocks(blockId, offset, size, PG, broken, true)
		for i := 0; i < len(blocks); i++ {
			if i != broken {
				blocks[i].Unref()
			}
		}
		return blocks[broken]
	}

	var wg sync.WaitGroup
	data := make([]common.IOBuffer, n)
	offs := service.encoder.GetRegenerateOffset(broken, offset, size)

	wg.Add(n - 1)
	for j := 0; j < n; j++ {
		if j != broken {
			go func(j int) {
				start := time.Now()
				var args stor.GetBatchDataArgs
				args.PGId = int(PG.PGId)
				for _, v := range offs {
					args.BlockId = append(args.BlockId, int16(blockId))
					args.Offset = append(args.Offset, v.Offset)
					args.Size = append(args.Size, uint32(v.Size))
				}
				conn := service.pool.GetConnectionByBrick(PG.Bricks[j])
				stor.Compress(&args)
				data[j] = getData(&args, conn, service.bufPool)

				logrus.Debugln("Got data with size", size/4, "for brick", j, "cost:", time.Since(start))
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
	logrus.Println("Regenerate data cost:", time.Since(start))
	return data_broken
}

func (service *GeometricService) Get(offset uint64, size uint64, PG dir.PlacementGroup, objectIndex index.ObjectIndex) chan common.IOBuffer {

	geoIndex := objectIndex.Geometric
	currentOff := uint64(0)

	var args stor.GetBatchDataArgs
	args.PGId = int(PG.PGId)

	for _, block := range geoIndex.Blocks {
		if currentOff+block.Size > offset && currentOff < offset+size {
			start := uint64(0)
			end := uint64(block.Size)
			if currentOff < offset {
				start = offset - currentOff
			}
			if currentOff+block.Size > offset+size {
				end = offset + size - currentOff
			}
			args.BlockId = append(args.BlockId, block.BlockID)
			args.Offset = append(args.Offset, block.OffsetInBucket+start)
			args.Size = append(args.Size, uint32(end-start))
		}
		currentOff += block.Size
	}
	conn := service.pool.GetConnectionByBrick(PG.Bricks[geoIndex.Blocks[0].IndexInPG])
	sizes := make([]uint32, 0)
	for off := uint64(0); off < size; off += common.StorReadMinSize {
		sizes = append(sizes, uint32(common.Min(common.StorReadMinSize, size-off)))
	}

	return getDataBySizes(&args, conn, service.bufPool, sizes, nil)

}

func (service *GeometricService) getMaxSize(blockID int16, pg dir.PlacementGroup) uint64 {
	k := service.k
	sizes := make([]uint64, k)
	var wg sync.WaitGroup
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func(i int) {
			conn := service.pool.GetConnectionByBrick(pg.Bricks[i])
			var args stor.GetBlockSizeArgs
			args.BlockId = blockID
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

func (service *GeometricService) GenerateParity(PG dir.PlacementGroup) {
	k := service.k
	n := k + service.redundancy

	blockSizes := make([]uint64, 0)
	blockSizes = append(blockSizes, 0)
	for blockSize := service.partitioner.MinBlock; blockSize <= service.partitioner.MaxBlock; blockSize = blockSize * uint64(service.partitioner.Base) {
		blockSizes = append(blockSizes, blockSize)
	}

	var wg sync.WaitGroup
	for _, blockSize := range blockSizes {
		blockId := service.partitioner.SizeToBlockId(blockSize)
		maxSize := service.getMaxSize(blockId, PG)
		logrus.Infof("Generating PG%d-block%d, size is %d\n", PG.PGId, blockId, maxSize)

		if blockId == 0 {
			blockSize = 4 << 20
		}

		for offset := uint64(0); offset < maxSize; offset += blockSize {
			size := blockSize
			if maxSize-offset < size {
				size = maxSize - offset
			}
			blocks := service.RepairBlocks(int(blockId), offset, size, PG, -1, false)
			for i := range blocks[:k] {
				blocks[i].Unref()
			}
			wg.Add(service.redundancy)
			for i := k; i < n; i++ {
				go func(i int) {
					conn := service.pool.GetConnectionByBrick(PG.Bricks[i])
					putSlide(blocks[i], 0, uint64(len(blocks[i].Data)), offset, blockId, PG.PGId, conn)
					blocks[i].Unref()
					wg.Done()
				}(i)
			}
			wg.Wait()
		}

	}

}

func (service *GeometricService) Put(objectID uint64, data common.IOBuffer, PG dir.PlacementGroup, objectIndex index.ObjectIndex) {
	geoIndex := objectIndex.Geometric
	var wg sync.WaitGroup
	wg.Add(len(geoIndex.Blocks))
	curOff := uint64(0)
	for _, block := range geoIndex.Blocks {
		func(block index.GeometricIndexBlock, offset uint64) {
			conn := service.pool.GetConnectionByBrick(PG.Bricks[block.IndexInPG])
			err := putSlide(data, offset, block.Size, block.OffsetInBucket, block.BlockID, PG.PGId, conn)
			if err != nil {
				logrus.Errorln(err)
			}
			wg.Done()
		}(block, curOff)
		curOff += block.Size
	}
	wg.Wait()
}

func (service *GeometricService) GetRecoverTasks(instruct dir.MoveInstruct) []dir.RecoveryTask {

	PG := instruct.PG
	broken := instruct.Broken
	toRecover := uint64(0)
	conn := service.pool.GetConnectionByBrick(PG.Bricks[broken])

	res := make([]dir.RecoveryTask, 0)

	blockSizes := make([]uint64, 0)
	for blockSize := service.partitioner.MaxBlock; blockSize >= service.partitioner.MinBlock; blockSize = blockSize / uint64(service.partitioner.Base) {
		blockSizes = append(blockSizes, blockSize)
	}
	blockSizes = append(blockSizes, 0)

	for _, blockSize := range blockSizes {
		blockId := service.partitioner.SizeToBlockId(blockSize)

		var args stor.GetBlockSizeArgs
		args.BlockId = blockId
		args.PGId = PG.PGId
		conn.Call("StorageService.GetBlockSize", &args, &toRecover)

		logrus.Warnf("PG%d-block%d: %u", PG.PGId, blockId, toRecover)

		if blockId == 0 {
			blockSize = 4 << 20
		}

		for offset := uint64(0); offset < toRecover; offset += blockSize {
			size := blockSize
			if blockId == 0 && offset+size > toRecover {
				size = toRecover - offset
			}
			res = append(res, dir.RecoveryTask{PGId: int(instruct.PG.PGId), Broken: instruct.Broken, NewBrick: instruct.NewBrick, BlockId: int(blockId), BlockOffset: offset, Size: size})
		}
	}
	return res
}

func (service *GeometricService) Recovery() {

	conn := service.pool.GetDirConnection()
	volume := service.pool.GetVolume()

	blocks := int(service.partitioner.MaxBlock / service.partitioner.MinBlock)
	maxWrites := make(chan bool, common.RecoveryConcurrentNum*4*blocks)
	for i := 0; i < common.RecoveryConcurrentNum*4*blocks; i++ {
		maxWrites <- true
	}

	maxTasks := make(chan bool, common.RecoveryConcurrentNum*blocks)
	for i := 0; i < common.RecoveryConcurrentNum*blocks; i++ {
		maxTasks <- true
	}

	for {
		var task dir.RecoveryTask
		err := conn.Call("DirectoryService.PullRecoveryTask", &volume.VolumeName, &task)
		if err != nil && err.Error() == common.ErrNoTaskLeft.Error() {
			break
		} else if err != nil {
			logrus.Errorln(err)
			time.Sleep(time.Second)
			continue
		}
		blocks := int(task.Size / service.partitioner.MinBlock)
		for i := 0; i < blocks; i++ {
			<-maxTasks
		}
		go func(task dir.RecoveryTask, blocks int) {
			logrus.Infoln("Handling recovery task", task)
			PGId := task.PGId
			broken := task.Broken
			dest := task.NewBrick

			var args stor.PutDataArgs
			args.BlockId = int16(task.BlockId)
			args.Offset = task.BlockOffset
			args.Len = task.Size

			volume := service.pool.GetVolume()
			pg := volume.PGs[task.PGId]

			data := service.RegenerateBlock(task.BlockId, task.BlockOffset, task.Size, broken, pg)
			args.PGId = uint32(PGId)

			//Time consuming, but with little concurrency. Should not block the running of repair.
			go func(brick uuid.UUID, args *stor.PutDataArgs, data common.IOBuffer, blocks int) {
				for i := 0; i < blocks; i++ {
					<-maxWrites
				}
				conn := service.pool.GetConnectionByBrick(brick)

				err := putSlide(data, 0, args.Len, args.Offset, args.BlockId, args.PGId, conn)
				if err != nil {
					logrus.Errorln(err)
				}
				data.Unref()
				for i := 0; i < blocks; i++ {
					maxWrites <- true
				}
			}(dest, &args, data, blocks)

			logrus.Infoln("Finish recovery task", task)
			for i := 0; i < blocks; i++ {
				maxTasks <- true
			}

		}(task, blocks)
	}
	for i := 0; i < common.RecoveryConcurrentNum*blocks; i++ {
		<-maxTasks
	}
	logrus.Infoln("Begin to wait for writing on disk.")
	for i := 0; i < common.RecoveryConcurrentNum*4*blocks; i++ {
		<-maxWrites
	}
}
