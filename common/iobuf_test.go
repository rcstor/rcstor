package common

import (
	"bytes"
	"testing"
	"time"
)

func BenchmarkGetBuf(b *testing.B) {
	pool := MakeIOBufferPool()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetBuffer(1 << 24)
		pool.ReleaseBuffer(buf)
	}
}

func TestBufferPool(t *testing.T) {
	pool := MakeIOBufferPool()
	size := 1 << 22
	buf := pool.GetBuffer(size)
	if len(buf.Data) != size {
		t.Fatal("Failed to alloc buffer with len", len(buf.Data))
	}
	size = 1 << 24

	bufChan := make(chan IOBuffer, 8)

	for i := 0; i < 8; i++ {
		go func() {
			bufChan <- pool.GetBuffer(size)
		}()
	}

	for i := 0; i < 8; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatal("Can not get buffer")
		case buf = <-bufChan:
			if len(buf.Data) != size {
				t.Fatal("Failed to alloc buffer with len", len(buf.Data))
			}
			for i := 0; i < len(buf.Data); i++ {
				buf.Data[i] = 0xac
			}
		}
	}

	if !bytes.Equal(buf.Data, bytes.Repeat([]byte{0xac}, size)) {
		t.Fatal("The data in iobuf is not correct")
	}

	go func() {
		bufChan <- pool.GetBuffer(size)
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("Time out, can not get the buf.")
	case buf = <-bufChan:
		pool.ReleaseBuffer(buf)
	}
}
