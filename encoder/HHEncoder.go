package encoder


import (
	log "github.com/sirupsen/logrus"
	"rcstor/common"
	"time"
	"unsafe"
)

// #cgo darwin LDFLAGS: ${SRCDIR}/libmsr_mac.a
// #cgo linux LDFLAGS: ${SRCDIR}/libmsr.a
// #include "hh.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

type HHEncoder struct {
	n, k int
	conf C.hh_conf
}

func (encoder *HHEncoder) Regenerate(input []common.IOBuffer, output common.IOBuffer) {
	if len(input) != encoder.n {
		log.Panic("Parameter error!")
	}

	cgoStart := time.Now()
	var b *C.char
	ptrSize := unsafe.Sizeof(b)

	input_ptr := C.malloc(C.size_t(encoder.n) * C.size_t(ptrSize))
	defer C.free(input_ptr)

	regSize := len(output.Data)
	broken := 0
	for i := 0; i < encoder.n; i++ {
		if input[i].Data != nil {
			size := len(input[i].Data)
			if size != 0 {
				pos := (*unsafe.Pointer)(unsafe.Pointer(uintptr(input_ptr) + uintptr(i)*ptrSize))
				*pos = unsafe.Pointer(&(input[i].Data[0]))
			}
		} else {
			pos := (*unsafe.Pointer)(unsafe.Pointer(uintptr(input_ptr) + uintptr(i)*ptrSize))
			*pos = unsafe.Pointer(uintptr(0x00))
			broken = i
		}
	}

	start := time.Now()

	var context C.hh_regenerate_context
	C.hh_get_regenerate_offset(C.int(regSize), &context, &encoder.conf, C.int(broken))
	//All go pointers should be passed through cgo directly.
	C.hh_regenerate(C.int(regSize), &context, &encoder.conf, (**C.uint8_t)(input_ptr), (*C.uint8_t)(unsafe.Pointer(&output.Data[0])))
	C.hh_free_regenerate_context(&encoder.conf, &context)

	log.Debugln("HH C Regenerate cost:", time.Since(start))
	log.Debugln("HH regenerate warper cost:", time.Since(cgoStart))

}

func (encoder *HHEncoder) Encode(input []common.IOBuffer, output []common.IOBuffer) {
	if len(input) != encoder.n {
		log.Panic("Parameter error!")
	}
	cgoStart := time.Now()
	var b *C.char
	ptrSize := unsafe.Sizeof(b)

	input_ptr := C.malloc(C.size_t(encoder.n) * C.size_t(ptrSize))
	defer C.free(input_ptr)

	size := 0
	survived := 0

	for i := 0; i < encoder.k; i++ {
		if input[i].Data != nil {
			size = len(input[i].Data)
			if size != 0 {
				pos := (*unsafe.Pointer)(unsafe.Pointer(uintptr(input_ptr) + uintptr(i)*ptrSize))
				C.posix_memalign(pos, 64, C.size_t(size))

				*pos = unsafe.Pointer(&(input[i].Data[0]))
			}
			survived++
		} else {
			pos := (*unsafe.Pointer)(unsafe.Pointer(uintptr(input_ptr) + uintptr(i)*ptrSize))
			*pos = unsafe.Pointer(uintptr(0x00))
		}
	}

	if survived != encoder.k {
		for i := 0; i < encoder.n; i++ {
			if input[i].Data != nil {
				log.Warn("given ", i, " ", len(input[i].Data))
			}
		}
		log.Fatalln("No enough output buffer is given", " survived=", survived, " len(output)=", len(output))
	}

	output_ptr := C.malloc(C.size_t(encoder.n-encoder.k) * C.size_t(ptrSize))
	defer C.free(output_ptr)

	for i := 0; i < len(output); i++ {
		element := (*unsafe.Pointer)(unsafe.Pointer(uintptr(output_ptr) + uintptr(i)*ptrSize))
		*element = unsafe.Pointer(&output[i].Data[0])
	}

	start := time.Now()
	C.hh_encode(C.int(size), &encoder.conf, (**C.uint8_t)(input_ptr), (**C.uint8_t)(output_ptr))
	for i := range output {
		if input[i+encoder.k].Data == nil {
			input[i+encoder.k] = output[i]
		}
	}

	log.Debugln("HH C encode cost:", time.Since(start))
	log.Debugln("HH encode warper cost:", time.Since(cgoStart))
}


func (encoder *HHEncoder) GetRegenerateOffset(broken int, offset, size uint64) (ret []OffAndSize) {
	//K := encoder.k
	//R := encoder.n - encoder.k
	var context C.hh_regenerate_context
	C.hh_get_regenerate_offset(C.int(size), &context, &encoder.conf, C.int(broken))
	var b C.int
	intSize := unsafe.Sizeof(b)
	ret = make([]OffAndSize, encoder.n)

	for i := range ret {
		//off := context.offset[i]
		off := *(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(context.offset)) + uintptr(i)*intSize))
		siz := *(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(context.size)) + uintptr(i)*intSize))
		ret[i] = OffAndSize{uint64(off), uint64(siz)}
	}
	C.hh_free_regenerate_context(&encoder.conf, &context)
	return
}
func MakeHHEncoder(n, k int) *HHEncoder {
	encoder := &HHEncoder{n: n, k: k}
	C.hh_init_with_default_allocator(&encoder.conf, C.int(n), C.int(k))
	return encoder
}

/*
func (encoder *HHEncoder) repairBlock(LV dir.PlacementGroup, pool *common.ConnectionPool, bufPool *common.IOBufferPool, broken int, blockId int16, offset, size uint64, ch *chan bool) common.IOBuffer {
	data := make([]common.IOBuffer, encoder.n)
	var wgGetBatchData sync.WaitGroup
	offs := encoder.GetRegenerateOffset(broken, offset, size)
	for i := 0; i < encoder.n; i++ {
		if i != broken {
			wgGetBatchData.Add(1)
			go func(i int) {
				var args MSRStorage.GetDataArgs
				args.PV = LV.Bricks[i]
				args.BlockId = blockId
				args.Offset = offs[i].Offset
				args.Size = uint32(offs[i].Size)
				conn := pool.GetConnection(args.PV.Addr())
				conn.Call("StorageService.GetBatchData", &args, &data[i], 10*time.Second)
				wgGetBatchData.Done()
			}(i)
		}
	}
	data_broken := bufPool.GetBufferNonBlocking(int(size))
	wgGetBatchData.Wait()
	if ch != nil {
		*ch <- true
	}
	encoder.FastRegenerate(data, data_broken)
	for i := range data {
		if i != broken {
			data[i].Unref()
		}

	}
	return data_broken
}
*/