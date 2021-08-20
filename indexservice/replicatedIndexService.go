package index

import (
	"rcstor/common"
)
type ReplicatedIndex struct {
	Offset uint64
	Size   uint64
}


type ReplicatedIndexConstructor struct {

}

func (constructor *ReplicatedIndexConstructor) Construct(args *PutIndexArgs,spaceUsed *interface{}) *ObjectIndex {
	index := &ObjectIndex{}

	if *spaceUsed == nil {
		*spaceUsed = uint64(0)
	}

	replicated := &ReplicatedIndex{
		Size:      args.Size,
		Offset:    (*spaceUsed).(uint64),
	}

	index.Replicated = replicated

	(*spaceUsed)= index.GetSize() + (*spaceUsed).(uint64)
	if (*spaceUsed).(uint64)%common.StorageOffsetAlign != 0 {
		(*spaceUsed) = (*spaceUsed).(uint64) + (common.StorageOffsetAlign - (*spaceUsed).(uint64)%common.StorageOffsetAlign)
	}
	return index
}
