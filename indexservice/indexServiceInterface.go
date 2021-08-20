package index

type GetIndexArgs struct {
	ObjectId uint64
	PGId      uint32
}

type PutIndexArgs struct {
	ObjectID uint64
	Size     uint64
	PGId     uint32
}

type ObjectIndex struct {
	Stripe  *StripedIndex
	Contiguous  *ContiguousIndex
	Replicated  *ReplicatedIndex
	Geometric *GeometricIndex
	StripeMax  *StripeMaxIndex
}

type IndexConstructor interface {
	Construct(args *PutIndexArgs,spaces *interface{}) *ObjectIndex
}

type IndexServiceInterface interface {
	GetIndex(args *GetIndexArgs, reply *ObjectIndex) error
	PutIndex(args *PutIndexArgs, reply *ObjectIndex) error
	Drop(args *int, reply *interface{}) error
	GetAllIndex(LVId *uint32, reply *[]ObjectIndex) error
}



func (objectIndex *ObjectIndex) GetSize() uint64 {
	if objectIndex.Contiguous != nil {
		return objectIndex.Contiguous.Size
	} else if objectIndex.Replicated != nil {
		return objectIndex.Replicated.Size
	} else if objectIndex.Geometric != nil {
		index := objectIndex.Geometric
		size := uint64(0)
		for _, v := range index.Blocks {
			size += v.Size
		}
		return size
	} else if objectIndex.StripeMax != nil {
		return objectIndex.StripeMax.Size
	} else if objectIndex.Stripe != nil {
		return objectIndex.Stripe.Size
	}

	return 0
}
