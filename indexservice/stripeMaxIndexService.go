package index


type StripeMaxIndex struct {
	Offset uint64
	BlockSize     uint64
	Size          uint64
}

type StripedMaxIndexConstructor struct {
	K         int
	Alignment uint64
}

func (constructor *StripedMaxIndexConstructor) Construct(args *PutIndexArgs, spaceUsed *interface{}) *ObjectIndex {
	index := &ObjectIndex{}
	if *spaceUsed == nil {
		*spaceUsed = uint64(0)
	}

	blockSize := (args.Size + uint64(constructor.K - 1)) / uint64(constructor.K)
	if blockSize%(constructor.Alignment) != 0 {
		blockSize += (constructor.Alignment) - blockSize%(constructor.Alignment)
	}

	striped := &StripeMaxIndex{
		Size:   args.Size,
		Offset: (*spaceUsed).(uint64),
		BlockSize: blockSize,
	}

	index.StripeMax = striped
	 *spaceUsed = (*spaceUsed).(uint64) + index.StripeMax.BlockSize
	return index
}
