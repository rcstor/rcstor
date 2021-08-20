package encoder

import (
	log "github.com/sirupsen/logrus"
	"rcstor/common"
	"time"
	//"unsafe"
	"unsafe"
)

// #cgo darwin LDFLAGS: ${SRCDIR}/libmsr_mac.a
// #cgo linux LDFLAGS: ${SRCDIR}/libmsr.a
// #include "msr.h"
// #include <stdlib.h>
// #include <string.h>
// #include <stddef.h>
import "C"

const MINIMUM_CHUNK_SIZE = 512 * 64

type OffAndSize struct {
	Offset uint64
	Size   uint64
}

type MSREncoder struct {
	conf C.msr_conf
	n, k int
	pool *common.IOBufferPool
}

func (encoder *MSREncoder) Regenerate(input []common.IOBuffer, output common.IOBuffer) {

	if len(input) != encoder.n {
		log.Panic("Encoder parameter error!")
	}

	cgoStart := time.Now()
	var b *C.char
	ptrSize := unsafe.Sizeof(b)

	input_ptr := C.malloc(C.size_t(encoder.n) * C.size_t(ptrSize))
	defer C.free(input_ptr)

	regSize := 0
	broken := 0

	for i := 0; i < encoder.n; i++ {
		if input[i].Data != nil {
			size := len(input[i].Data)
			if size != 0 {
				regSize = size
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
	var context C.msr_regenerate_context
	C.msr_fill_regenerate_context(&context, &encoder.conf, C.int(broken))
	buf := encoder.pool.GetBuffer(int(context.regenerate_buf_size))
	defer buf.Unref()
	C.msr_regenerate(C.int(regSize), &context, &encoder.conf, (*C.uint8_t)(&buf.Data[0]),
		(**C.uint8_t)(input_ptr), (*C.uint8_t)(unsafe.Pointer(&output.Data[0])))
	C.msr_free_regenerate_context(&encoder.conf, &context)
	//All go pointers should be passed through cgo directly.

	log.Debugln("MSR C Regenerate cost:", time.Since(start))
	log.Debugln("MSR regenerate warper cost:", time.Since(cgoStart))
}


func (encoder *MSREncoder) GetRegenerateOffset(broken int, offset uint64, size uint64) []OffAndSize {
	var context C.msr_regenerate_context

	offsets := make([]C.int, encoder.conf.beta)
	C.msr_fill_regenerate_context(&context, &encoder.conf, C.int(broken))
	C.msr_get_regenerate_offset(C.int(size), &context, &encoder.conf, (*C.int)(&offsets[0]))

	res := make([]OffAndSize, encoder.conf.beta)
	for i := range res {
		res[i].Offset = uint64(offsets[i])
		res[i].Size = size / uint64(encoder.conf.alpha)
	}
	C.msr_free_regenerate_context(&encoder.conf, &context)
	for i := range res {
		res[i].Offset += offset
	}
	return res
}

func (encoder *MSREncoder) Encode(input []common.IOBuffer, output []common.IOBuffer) {
	if len(input) != encoder.n {
		log.Panic("Parameter error!")
	}
	cgoStart := time.Now()
	var b *C.uint8_t
	ptrSize := unsafe.Sizeof(b)

	input_ptr := C.malloc(C.size_t(encoder.n) * C.size_t(ptrSize))
	defer C.free(input_ptr)

	size := 0
	survived := 0
	for i := 0; i < encoder.n; i++ {
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

	if encoder.n-survived != len(output) {
		for i := 0; i < encoder.n; i++ {
			if input[i].Data != nil {
				log.Warn("given ", i, " ", len(input[i].Data))
			}
		}
		log.Fatalln("No enough output buffer is given", " survived=", survived, " len(output)=", len(output))
	}
	var context C.msr_encode_context
	C.msr_fill_encode_context(&context, &encoder.conf, (**C.uint8_t)(input_ptr))
	defer C.msr_free_encode_context(&encoder.conf, &context)

	output_ptr := C.malloc(C.size_t(encoder.n-encoder.k) * C.size_t(ptrSize))
	defer C.free(output_ptr)

	for i := 0; i < len(output); i++ {
		element := (*unsafe.Pointer)(unsafe.Pointer(uintptr(output_ptr) + uintptr(i)*ptrSize))
		*element = unsafe.Pointer(&output[i].Data[0])
	}
	buf := encoder.pool.GetBuffer(int(context.encoding_buf_size))
	defer buf.Unref()

	start := time.Now()
	C.msr_encode(C.int(size), &context, &encoder.conf, (*C.uint8_t)(&buf.Data[0]), (**C.uint8_t)(input_ptr), (**C.uint8_t)(output_ptr))
	sid := 0
	for i := 0; i < encoder.n; i++ {
		if input[i].Data == nil {
			input[i] = output[sid]
			sid++
		}
	}
	log.Debugln("MSR C encode cost:", time.Since(start))
	log.Debugln("MSR encode warper cost:", time.Since(cgoStart))
}


func MakeMSREncoder(n, k int, pool *common.IOBufferPool) *MSREncoder {
	encoder := &MSREncoder{n: n, k: k, pool: pool}
	C.msr_init_with_default_allocator(&encoder.conf, C.int(n), C.int(k))
	return encoder
}
