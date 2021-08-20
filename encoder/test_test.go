package encoder

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"math/rand"
	"rcstor/common"
	"testing"
)

func TestLRCEncoder(t *testing.T) {

	logrus.Println("Testing LRC Encoder.")
	pool := common.MakeIOBufferPool()
	k := 10
	n := 14
	enc:= MakeLRCEncoder(k, 2, 2)
	data := make([]common.IOBuffer, n)
	size := 1 << 27
	for i := 0; i < k; i++ {
		data[i] = pool.GetBuffer(size)
		rand.Read(data[i].Data)
	}
	enc.Encode(data,pool)
	logrus.Println("...Passed")
}

func TestMSREncoder(t *testing.T) {

	logrus.Println("Testing MSR Encoder")
	n := 14
	k := 10


	data := make([]common.IOBuffer, n)

	pool := common.MakeIOBufferPool()

	encoder := MakeMSREncoder(n,k,pool)

	size := (1 << 27)
	for i := 0; i < k; i++ {
		data[i] = pool.GetBuffer(size)
		rand.Read(data[i].Data)
	}
	output := make([]common.IOBuffer, n-k)
	for i := 0; i < n-k; i++ {
		output[i] = pool.GetBuffer(size)
	}

	encoder.Encode(data, output)

	survived := make([]common.IOBuffer,n)
	decoded := make([]common.IOBuffer,n-k)
	for i:=n-k;i<n;i++{
		survived[i] = data[i]
	}
	for i:=0;i<n-k;i++ {
		decoded[i] = pool.GetBuffer(size)
	}

	encoder.Encode(survived, decoded)

	for i := 0; i < n-k; i++ {
		if !bytes.Equal(decoded[i].Data, data[i].Data) {
			t.Fatal("Data decodeded is not correct.")
		}
	}

	for broken := 0; broken < n; broken++ {
		off := encoder.GetRegenerateOffset(broken, 0, uint64(size))
		reg := make([]common.IOBuffer, n)
		for i := 0; i < n; i++ {
			if i != broken {
				reg[i] = pool.GetBuffer(size / (n - k))
			}
		}
		for i := 0; i < n; i++ {
			if i != broken {
				curOff := uint64(0)
				for j := 0; j < len(off); j++ {
					copy(reg[i].Data[curOff:curOff + off[j].Size], data[i].Data[off[j].Offset:off[j].Offset+ off[j].Size])
					curOff += off[j].Size
				}
			}
		}

		output := pool.GetBuffer(size)

		encoder.Regenerate(reg, output)
		if !bytes.Equal(output.Data, data[broken].Data) {
			t.Fatal("Data regenerated is not correct.")
		}
		output.Unref()
		for i := 0; i < n; i++ {
			if i != broken {
				reg[i].Unref()
			}
		}
	}

	logrus.Println("...Passed")

}

func TestHHEncoder(t *testing.T) {

	logrus.Println("Testing HH Encoder")
	n := 14
	k := 10

	data := make([]common.IOBuffer, n)

	pool := common.MakeIOBufferPool()
	size := (1 << 27)
	for i := 0; i < k; i++ {
		data[i] = pool.GetBuffer(size)
		rand.Read(data[i].Data)
	}
	output := make([]common.IOBuffer, n-k)
	for i := 0; i < n-k; i++ {
		output[i] = pool.GetBuffer(size)
	}

	encoder := MakeHHEncoder(n, k)
	encoder.Encode(data, output)

	for broken := 0; broken < n; broken++ {
		off := encoder.GetRegenerateOffset(broken, 0, uint64(size))
		reg := make([]common.IOBuffer, n)
		for i := 0; i < n; i++ {
			if i != broken {
				reg[i] = pool.GetBuffer(int(off[i].Size))
			}
		}
		for i := 0; i < n; i++ {
			if i != broken {
				copy(reg[i].Data, data[i].Data[off[i].Offset:off[i].Offset+off[i].Size])

			}
		}

		output := pool.GetBuffer(size)

		encoder.Regenerate(reg, output)
		if !bytes.Equal(output.Data, data[broken].Data) {
			t.Fatal("Data regenerated is not correct.")
			//fmt.Println("Data regenerated is not correct.")
		}

		output.Unref()
		for i := 0; i < n; i++ {
			if i != broken {
				reg[i].Unref()
			}
		}
	}

	logrus.Println("...Passed")
}

func TestRSEncoder(t *testing.T) {

	logrus.Println("Testing RS Encoder")
	n := 14
	k := 10

	data := make([]common.IOBuffer, n)

	pool := common.MakeIOBufferPool()

	encoder := MakeRSEncoder(n,k)

	size := rand.Intn(1004032)
	for i := 0; i < k; i++ {
		data[i] = pool.GetBuffer(size)
		rand.Read(data[i].Data)
	}
	output := make([]common.IOBuffer, n-k)
	for i := 0; i < n-k; i++ {
		output[i] = pool.GetBuffer(size)
	}

	encoder.Encode(data, output)

	survived := make([]common.IOBuffer,n)
	decoded := make([]common.IOBuffer,n-k)
	for i:=n-k;i<n;i++{
		survived[i] = data[i]
	}
	for i:=0;i<n-k;i++ {
		decoded[i] = pool.GetBuffer(size)
	}

	encoder.Encode(survived, decoded)

	for i := 0; i < n-k; i++ {
		if !bytes.Equal(decoded[i].Data, data[i].Data) {
			t.Fatal("Data decodeded is not correct.")
		}
	}

	logrus.Println("...Passed")
}