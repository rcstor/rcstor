package index

type StripedIndex struct {
	Offset uint64
	Size   uint64
}

type StripedIndexConstructor struct {
	StripSize uint64
	K         int
}

func (constructor *StripedIndexConstructor) Construct(args *PutIndexArgs, spaceUsed *interface{}) *ObjectIndex {
	index := &ObjectIndex{}
	if *spaceUsed == nil {
		*spaceUsed = uint64(0)
	}

	striped := &StripedIndex{
		Size:   args.Size,
		Offset: (*spaceUsed).(uint64),
	}

	index.Stripe = striped
	size := uint64(index.GetSize())
	k := constructor.K
	blockSize := constructor.StripSize
	left := size % (blockSize * uint64(k))
	minSize := uint64(256 * k)

	padding := minSize - left%minSize
	finalSize := (size + padding) / uint64(k)

	(*spaceUsed)= (*spaceUsed).(uint64) + finalSize

	return index
}
