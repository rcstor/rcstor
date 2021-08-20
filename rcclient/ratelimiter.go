package rcclient

import (
	"context"
	"golang.org/x/time/rate"
	"io"
)

type RateLimitWriter struct {
	writer io.Writer
	limits uint64
}

func RateWriter(w io.Writer,limit uint64) io.Writer{
	return &RateLimitWriter{writer: w,limits: limit}
}

func (writer *RateLimitWriter) Write(p []byte) (n int, err error) {
	packetSize := 16<<10
	limits := writer.limits / uint64(packetSize)
	limiter := rate.NewLimiter(rate.Limit(limits),1)
	written := 0

	for i:=0;i<(len(p) + packetSize - 1)/packetSize;i++ {
		limiter.Wait(context.Background())
		tail := (i+1) * packetSize
		if tail > len(p) {
			tail = len(p)
		}
		n,err := writer.writer.Write(p[i*packetSize:tail])
		written += n
		if err != nil {
			return written,err
		}
	}
	return  written,nil
}