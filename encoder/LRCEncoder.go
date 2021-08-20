package encoder

import (
	log "github.com/sirupsen/logrus"
	"rcstor/common"
	"time"
)

type LRCEncoder struct {
	k, glo, loc int
	gloEncoder  *RSEncoder
	locEncoder  *RSEncoder
}

func MakeLRCEncoder(k, glo, loc int) *LRCEncoder {
	encoder := &LRCEncoder{k: k, glo: glo, loc: loc}
	encoder.locEncoder = MakeRSEncoder(k/loc+1, k/loc)
	encoder.gloEncoder = MakeRSEncoder(k+glo, k)
	return encoder
}

func (encoder *LRCEncoder) LocalParityNum() int {
	return encoder.k / encoder.loc
}
func (encoder *LRCEncoder) Loc() int {
	return encoder.loc
}
func (encoder *LRCEncoder) Glo() int {
	return encoder.glo
}

func (encoder *LRCEncoder) NumNeed(want int) int {
	if want < encoder.k {
		return encoder.LocalParityNum()
	} else if want < encoder.k+encoder.glo {
		return encoder.k
	} else {
		return encoder.LocalParityNum()
	}
}

func (encoder *LRCEncoder) NumDecoded(want int) int {
	if want < encoder.k {
		return 1
	} else if want < encoder.k+encoder.glo {
		return encoder.glo
	} else {
		return 1
	}
}

func (encoder *LRCEncoder) Need(want int, i int) bool {
	if i == want {
		return false
	} else if want < encoder.k {
		return want/encoder.LocalParityNum() == i/encoder.LocalParityNum() ||
			i-encoder.glo-encoder.k == want/encoder.LocalParityNum()
	} else if want < encoder.k+encoder.glo {
		return i < encoder.k+encoder.glo
	} else {
		return i/encoder.LocalParityNum() == want-encoder.k-encoder.glo
	}
}

func (encoder *LRCEncoder) Encode(shards []common.IOBuffer, pool *common.IOBufferPool) {
	glo := encoder.glo
	loc := encoder.loc
	k := encoder.k
	interval := k / loc
	a := make([]common.IOBuffer, k+glo)
	for i := 0; i < k+glo; i++ {
		a[i] = shards[i]
	}
	outputs := make([]common.IOBuffer, glo)
	for i := 0; i < glo; i++ {
		outputs[i] = pool.GetBuffer(len(a[0].Data))
	}
	encoder.gloEncoder.Encode(a, outputs)
	for i := 0; i < glo; i++ {
		shards[k+i] = a[k+i]
	}

	for i, start := 0, 0; i < loc; i++ {

		func(i, start int) {

			b := make([]common.IOBuffer, interval+1)
			for j := 0; j < interval; j++ {
				b[j] = shards[start+j]
			}
			b[interval] = shards[k+glo+i]

			outputs := make([]common.IOBuffer, 1)
			outputs[0] = pool.GetBuffer(len(b[0].Data))

			encoder.locEncoder.Encode(b, outputs)

			shards[k+glo+i] = b[interval]
		}(i, start)
		start += interval
	}

}

func (encoder *LRCEncoder) trans(i, want int) int {
	if want < encoder.k {
		if i < encoder.LocalParityNum() {
			return i + want/encoder.LocalParityNum()*encoder.LocalParityNum()
		} else {
			return encoder.k + encoder.glo + want/encoder.LocalParityNum()
		}
	} else if want < encoder.k+encoder.glo {
		return i
	} else {
		if i < encoder.LocalParityNum() {
			return i + (want-encoder.k-encoder.glo)*encoder.LocalParityNum()
		} else {
			return want
		}
	}
}

func (encoder *LRCEncoder) ReconstructBuffer(input []common.IOBuffer, output []common.IOBuffer, want int) {
	k := encoder.k
	startTime := time.Now()
	data := make([]common.IOBuffer, encoder.NumNeed(want)+encoder.NumDecoded(want))
	for i := 0; i < len(data); i++ {
		j := encoder.trans(i, want)
		data[i] = input[j]
	}
	if want < k || want >= encoder.k+encoder.glo {
		encoder.locEncoder.Encode(data, output)
	} else {
		encoder.gloEncoder.Encode(data, output)
	}
	log.Println("LRC Regenerate cost:", time.Since(startTime))
	for i := 0; i < len(data); i++ {
		j := encoder.trans(i, want)
		input[j] = data[i]
	}
}
