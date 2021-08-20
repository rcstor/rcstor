package index

import (
	"rcstor/common"
)

type GeometricIndexBlock struct {
	IndexInPG      int16
	BlockID        int16
	OffsetInBucket uint64
	Size           uint64
}

type GeometricIndex struct {
	Blocks    []GeometricIndexBlock
}

type GeometricIndexConstructor struct {
	Partitioner GeometricPartitioner
	K           int
}

func (constructor *GeometricIndexConstructor) Construct(args *PutIndexArgs,spaceUsed *interface{}) *ObjectIndex {
	sizeList := constructor.Partitioner.Split(args.Size)

	k := constructor.K
	if *spaceUsed == nil {
		*spaceUsed = make([][]uint64, k)
		spaces := (*spaceUsed).([][]uint64)
		for i:=0;i<k;i++{
			spaces[i] = make([]uint64,constructor.Partitioner.SizeToBlockId(constructor.Partitioner.MaxBlock) + 1)
		}
	}
	spaces := (*spaceUsed).([][]uint64)

	index := &GeometricIndex{}
	index.Blocks = make([]GeometricIndexBlock,0)
	disk := constructor.getMostFreeBrick(spaces,constructor.Partitioner.SizeToBlockId(sizeList[len(sizeList) - 1]))
	for _,size := range sizeList {
		block := &GeometricIndexBlock{}
		block.Size = size
		block.BlockID = constructor.Partitioner.SizeToBlockId(size)

		block.IndexInPG = disk
		block.OffsetInBucket = spaces[block.IndexInPG][block.BlockID]

		index.Blocks = append(index.Blocks,*block)
		spaces[block.IndexInPG][block.BlockID] += block.Size
		if spaces[block.IndexInPG][block.BlockID]%common.StorageOffsetAlign != 0 {
			spaces[block.IndexInPG][block.BlockID] += common.StorageOffsetAlign - spaces[block.IndexInPG][block.BlockID]%common.StorageOffsetAlign
		}
	}

	return &ObjectIndex{Geometric: index}
}

func (constructor *GeometricIndexConstructor) getMostFreeBrick(spaceUsed [][]uint64, blockId int16) int16 {
	ret := 0
	for i := 1; i < len(spaceUsed); i++ {
		if spaceUsed[i][blockId] < spaceUsed[ret][blockId] {
			ret = i
		}
	}
	return int16(ret)
}

