package common

import (
	log "github.com/sirupsen/logrus"
	"reflect"
	"sync"
	"unsafe"
)

//#include <stdlib.h>
import "C"

//The memory of IOBuffer is in C code. Do not forget to unref the buffer after use!
//Not thread safe.
type IOBuffer struct {
	Data        []byte
	refCount    int32
	bytesToSend int
	pool        *IOBufferPool

	//Used to free memory
	allocateAddr uintptr
}

const PAGE_SIZE = 1 << 12

func allocIOBuffer(size int, pool *IOBufferPool) (res IOBuffer) {
	//log.Debugln("Buffer allocated",size)
	res.refCount = 1
	buf := C.valloc(C.size_t(roundUp(size)))

	res.Data = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(buf), Len: int(size), Cap: int(roundUp(size))}))
	res.pool = pool
	res.bytesToSend = size
	res.allocateAddr = uintptr(buf)
	return
}

func freeIOBuffer(buffer IOBuffer) {
	if len(buffer.Data) != 0 {
		C.free(unsafe.Pointer(buffer.allocateAddr))
	}
}

// Do not alloc(A);alloc(B);free(B);free(A) from a specific pool, deadlock possible.
//

func (buf *IOBuffer) SetBytesToSend(size int) {
	buf.bytesToSend = size
}

func (buf *IOBuffer) Ref() {
	buf.refCount++
}

func (buf *IOBuffer) Unref() {
	buf.refCount--
	if buf.refCount == 0 {
		if buf.pool != nil {
			buf.pool.ReleaseBuffer(*buf)
		} else {
			freeIOBuffer(*buf)
		}
	}
}

func (buf IOBuffer) Slice(begin,end uint64) IOBuffer{
	buf.Data = buf.Data[begin:end]
	return buf
}

var iobufSize = []int{1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 1 << 19, 1 << 20, 1 << 21, 1 << 22, 1 << 23, 1 << 24, 1 << 25, 1 << 26, 1 << 27, 1 << 28, 1 << 29, 1 << 30}
var size2Bucket = map[int]int{
	1 << 12: 0,
	1 << 13: 1,
	1 << 14: 2,
	1 << 15: 3,
	1 << 16: 4,
	1 << 17: 5,
	1 << 18: 6,
	1 << 19: 7,
	1 << 20: 8,
	1 << 21: 9,
	1 << 22: 10,
	1 << 23: 11,
	1 << 24: 12,
	1 << 25: 13,
	1 << 26: 14,
	1 << 27: 15,
	1 << 28: 16,
	1 << 29: 17,
	1 << 30: 18,
}

var iobufCount = []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
var iobufMaxCount = []int{128, 128, 128, 128, 128, 128, 128, 128, 128, 64, 64, 64, 32, 16, 8, 8, 8, 8, 8}

type IOBufferPool struct {
	chans     []chan IOBuffer
	allocated []int
	free      []int
	mutex     sync.Mutex
}

//There should be multiple pools rather than single pool to prevent from dead lock.
func MakeIOBufferPool() *IOBufferPool {
	res := IOBufferPool{}
	res.chans = make([]chan IOBuffer, len(iobufMaxCount))
	for i := 0; i < len(iobufCount); i++ {
		res.chans[i] = make(chan IOBuffer, iobufMaxCount[i])
		for j := 0; j < iobufCount[i]; j++ {
			res.chans[i] <- allocIOBuffer(iobufSize[i], &res)
		}
	}
	res.allocated = make([]int, len(iobufCount))
	res.free = make([]int, len(iobufCount))
	for i := 0; i < len(iobufCount); i++ {
		res.allocated[i] = iobufCount[i]
		res.free[i] = res.allocated[i]
	}

	return &res
}

func roundUp(size int) int {
	res := size - 1
	res |= res >> 1
	res |= res >> 2
	res |= res >> 4
	res |= res >> 8
	res |= res >> 16
	if res+1 < (1 << 12) {
		return 1 << 12
	} else {
		return res + 1
	}
}

func sizeToBucket(size int) int {

	if size <= 0 {
		return -1
	}

	bucket, ok := size2Bucket[roundUp(size)]
	if !ok {
		log.Fatalln("Trying to get an iobuf which is not in bucket, size=", size, " roundUpSize=", roundUp(size))
	}
	return bucket
}

func (pool *IOBufferPool) GetBufferNonBlocking(size int) IOBuffer {
	bucket := sizeToBucket(size)
	if bucket == -1 {
		return allocIOBuffer(0, pool)
	}
	select {
	case buf := <-pool.chans[bucket]:
		buf.refCount = 1
		buf.Data = buf.Data[:size]
		buf.bytesToSend = size
		return buf
	default:
		return allocIOBuffer(size, pool)
	}
}

func (pool *IOBufferPool) GetBuffer(size int) IOBuffer {
	return pool.GetBufferNonBlocking(size)
}

func (pool *IOBufferPool) ReleaseBuffer(buf IOBuffer) {

	bucket := sizeToBucket(len(buf.Data))
	if bucket == -1 || buf.allocateAddr != uintptr(unsafe.Pointer(&buf.Data[0])){
		freeIOBuffer(buf)
		return
	}
	buf.Data = buf.Data[:cap(buf.Data)]
	select {
	case pool.chans[bucket] <- buf:
		pool.mutex.Lock()
		pool.free[bucket]++
		pool.mutex.Unlock()
		return
	default:
		freeIOBuffer(buf)
		return
	}

}
