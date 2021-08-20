package common

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"testing"
)

type TestService struct {
	pool *IOBufferPool
}

type TestWriteArgs struct {
	Header int
	Bufx   IOBuffer
	Tail   int
	//Avoid multiple buffers in 1 rpc.
	//Bufy	IOBuffer
}

//The reference of iobuf will be reduced by one automatically after the reply has been written to network successfully.
func (service *TestService) TestRead(args *int, reply *IOBuffer) error {
	size := *args
	*reply = service.pool.GetBufferNonBlocking(size)
	copy(reply.Data, bytes.Repeat([]byte{0xaa}, size))
	return nil
}

func (service *TestService) TestWrite(args *TestWriteArgs, reply *int) error {

	//The buffer should be unrefed manually Here.
	defer args.Bufx.Unref()
	if args.Header != 0xc0 || args.Tail != 0xc1 || !bytes.Equal(args.Bufx.Data, bytes.Repeat([]byte{0xaa}, len(args.Bufx.Data))) {
		return ErrRead
	}
	*reply = 0

	return nil
}

func init() {
	log.SetLevel(log.DebugLevel)
	port := 12345
	addr := "0.0.0.0:" + fmt.Sprintf("%d", port)
	s, l, _ := StartRPCServer(addr)
	test := &TestService{}
	test.pool = MakeIOBufferPool()
	s.RegisterName("TestService", test)
	listener,_ := net.ListenTCP(addr,l)
	//debug.SetGCPercent(10)

	go ServeRPCServer(s, listener,func(closer net.Conn){})
}

func TestGetConnection(t *testing.T) {
	conn := MakeConnection("0.0.0.0:12345")
	var wg sync.WaitGroup
	wg.Add(1000)
	for i:=0;i<1000;i++ {
		go func() {
			conn.GetDirectConn()
			//conn.ReturnDirect(rwc)
			wg.Done()
		}()
	}
	wg.Wait()

}
func TestIOBufRead(t *testing.T) {

	conn := MakeConnection("127.0.0.1:12345")

	var wg sync.WaitGroup

	concurrent := 100

	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func(i int) {
			var args int = i
			var reply IOBuffer
			conn.Call("TestService.TestRead", &args, &reply)
			//log.Println("Read Request", i, "finished.")
			if !bytes.Equal(reply.Data, bytes.Repeat([]byte{0xaa}, args)) {
				log.Fatalln("Can not get the correct data", reply.Data)
			}
			reply.Unref()
			wg.Done()
		}(i)
	}

	wg.Wait()

	return
}

func TestIOBufWrite(t *testing.T) {
	conn := MakeConnection("127.0.0.1:12345")

	pool := MakeIOBufferPool()
	var wg sync.WaitGroup

	concurrent := 100

	var args TestWriteArgs
	args.Header = 0xc0
	args.Tail = 0xc1

	size := 1 << 25

	args.Bufx = pool.GetBuffer(size)
	copy(args.Bufx.Data, bytes.Repeat([]byte{0xaa}, size))

	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func(i int) {
			var reply int
			err := conn.Call("TestService.TestWrite", &args, &reply)
			if err != nil {
				t.Fatal(err)
			}
			//log.Println("Write Request", i, "finished.")

			wg.Done()
		}(i)
	}

	wg.Wait()
	//args.Bufx.Unref()
}
