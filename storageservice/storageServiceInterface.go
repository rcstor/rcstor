package stor

import (
	"net"
	"rcstor/common"
)

type GetDataArgs struct {
	//Negative id to represent a null block.
	BlockId int16

	Offset uint64
	Size   uint32
	PGId   uint32
}

type GetBatchDataArgs struct {

	BlockId []int16
	Offset  []uint64
	Size    []uint32
	//Ignore it, will be set automatically.
	NetSizes []uint32
	PGId   int
}

//Return the result of data concentrated.

type PutDataArgs struct {
	BlockId int16
	Offset  uint64
	Len     uint64
	//Data    common.IOBuffer
	PGId   uint32
}

type PutDataReply int

type GetBlockSizeArgs struct {
	BlockId int16
	PGId   uint32
}

type StorageServiceInterface interface {
	GetData(args *GetDataArgs, reply *common.IOBuffer) error
	GetBatchData(args *GetBatchDataArgs, reply *common.IOBuffer) error
	GetBlockSize(args *GetBlockSizeArgs, reply *uint64) error
	//PutData(args *PutDataArgs, reply *PutDataReply) error
	HandleDirect(rwc net.Conn)
}
