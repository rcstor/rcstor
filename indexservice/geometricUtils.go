package index

import (
	"log"
	"sort"
)


type GeometricPartitioner struct{
	MinBlock	uint64
	MaxBlock 	uint64
	Base        int
}

func (partitioner *GeometricPartitioner) SizeToBlockId(size uint64) int16{
	if size < partitioner.MinBlock {
		return 0
	}
	size = size / partitioner.MinBlock
	current := uint64(1)
	for id:=int16(1);current <= size;id++ {
		if size == current {
			return id
		}
		current = current * uint64(partitioner.Base)
	}
	log.Fatalln("Unexpected block size.")
	return 0
}


func (partitioner *GeometricPartitioner) blockIdToSize(blockId int) uint64{
	ret := partitioner.MinBlock
	for i:=0;i<blockId;i++{
		ret = ret * uint64(partitioner.Base)
	}
	return ret
}

func (partitoner *GeometricPartitioner) Split(size uint64) []uint64 {
	list := make([]uint64,0)
	for blockSize := partitoner.MinBlock; blockSize <= partitoner.MaxBlock && blockSize <= size; blockSize = blockSize * uint64(partitoner.Base) {
		list = append(list, blockSize)
		size -= blockSize
	}
	for size >= partitoner.MaxBlock {
		list = append(list, partitoner.MaxBlock)
		size -= partitoner.MaxBlock
	}
	for i := len(list) - 1; i >= 0; i-- {
		for size >= list[i] {
			list = append(list, list[i])
			size -= list[i]
		}
	}

	if size >= 0 {
		list = append(list, size)
	}

	ll := uint64List{list}
	ll.Sort()
	return ll.v
}


type uint64List struct {
	v []uint64
}

func (u *uint64List) Sort() {
	sort.Sort(u)
}

func (u *uint64List) Less(i, j int) bool {
	return u.v[i] < u.v[j]
}

func (u *uint64List) Len() int {
	return len(u.v)
}

func (u *uint64List) Swap(i, j int) {
	u.v[i], u.v[j] = u.v[j], u.v[i]
}
