package index


type ContiguousIndex struct {
	IndexInPG int16

	Offset uint64
	Size   uint64
}

type ContiguousIndexConstructor struct {
	K int
}

func (constructor *ContiguousIndexConstructor) Construct(args *PutIndexArgs,spaceUsed *interface{}) *ObjectIndex {
	index := &ObjectIndex{}
	k := constructor.K
	if *spaceUsed == nil {
		*spaceUsed = make([]uint64, k)
	}
	choosed := 0
	space := (*spaceUsed).([]uint64)
	for i := 0; i < k; i++ {
		if space[i] < space[choosed] {
			choosed = i
		}
	}
	contiguous := &ContiguousIndex{
		IndexInPG: int16(choosed),
		Size:      args.Size,
		Offset:    space[choosed],
	}

	index.Contiguous = contiguous

	space[choosed] += index.GetSize()

	return index
}