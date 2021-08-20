package encoder
import "C"
import (
	log "github.com/sirupsen/logrus"
	"rcstor/common"
	"time"
	"unsafe"
)

// #cgo darwin LDFLAGS: ${SRCDIR}/libmsr_mac.a
// #cgo linux LDFLAGS: ${SRCDIR}/libmsr.a
// #include "rs.h"
// #include <stdlib.h>
// #include <string.h>
// #include <stddef.h>
import "C"

type RSEncoder struct {
	conf C.rs_conf
	n, k int
}

func (encoder *RSEncoder) Encode(input []common.IOBuffer, output []common.IOBuffer) {
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

	brokens := make([]int,0)

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
			brokens = append(brokens,i)
		}
	}

	if len(brokens) == 0 {
		return
	}

	if encoder.n-survived != len(output) {
		for i := 0; i < encoder.n; i++ {
			if input[i].Data != nil {
				log.Warnln("given ", i, " ", len(input[i].Data))
			}
		}
		log.Fatalln("No enough output buffer is given", " survived=", survived, " len(output)=", len(output))
	}

	int_size := 4
	c_brokens := (*C.int)(C.malloc(C.size_t(int_size) * C.size_t(len(brokens))))
	defer C.free(unsafe.Pointer(c_brokens))

	for i:=0;i<len(brokens);i++{
		pos := (*C.int)(unsafe.Pointer(uintptr(unsafe.Pointer(c_brokens)) + uintptr(i*int_size)))
		*pos = C.int(brokens[i])
	}

	var context C.rs_decode_context
	C.rs_decode_context_init(&encoder.conf, &context,c_brokens,(C.int)(len(brokens)))

	defer C.rs_free_decode_context(&encoder.conf, &context)

	output_ptr := C.malloc(C.size_t(encoder.n-encoder.k) * C.size_t(ptrSize))
	defer C.free(output_ptr)

	for i := 0; i < len(output); i++ {
		element := (*unsafe.Pointer)(unsafe.Pointer(uintptr(output_ptr) + uintptr(i)*ptrSize))
		*element = unsafe.Pointer(&output[i].Data[0])
	}

	start := time.Now()

	C.rs_decode(C.int(size), &encoder.conf, &context, (**C.uint8_t)(input_ptr), (**C.uint8_t)(output_ptr))

	sid := 0
	for i := 0; i < encoder.n; i++ {
		if input[i].Data == nil {
			input[i] = output[sid]
			sid++
		}
	}
	log.Debugln("RS C encode cost:", time.Since(start))
	log.Debugln("RS encode warper cost:", time.Since(cgoStart))
}


func MakeRSEncoder(n, k int) *RSEncoder {
	encoder := &RSEncoder{n: n, k: k}
	C.rs_init_with_default_allocator(&encoder.conf, C.int(n), C.int(k))
	return encoder
}

