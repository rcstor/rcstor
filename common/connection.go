package common

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"rcstor/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const RPCByte byte = 0xa1
const DirectByte byte = 0xa2

type ConnectionPingReplyer struct {
}

type Connection struct {
	address atomic.Value

	mu   sync.Mutex

	directCnt int
	directChan chan io.ReadWriteCloser

	rpcChan chan *rpc.Client
	rpcCnt int

	pool *IOBufferPool
}

func (conn *ConnectionPingReplyer) Heartbeat(args *int, reply *int) error {
	//fmt.Printf("Heart beat received from %s\n", conn.address)
	return nil
}

func (conn *Connection) ResetAddress(addr string)  {
	conn.address.Store(addr)
}

func (conn *Connection) DropDirectConn() {
	conn.mu.Lock()
	conn.directCnt--
	conn.mu.Unlock()
}

func (conn *Connection) GetDirectConn() (rwc io.ReadWriteCloser) {
	select {
	case rwc = <-conn.directChan:
		return rwc
	default:
		//When too many clients call simultaenously
		conn.mu.Lock()
		if conn.directCnt < MaxTCPPerConn {
			rwc = conn.dialWithRetry()
			conn.directCnt++
			conn.mu.Unlock()
			n, err := rwc.Write([]byte{DirectByte})
			if n!=1 || err !=nil {
				log.Errorln(err)
			}
			return rwc
		} else {
			conn.mu.Unlock()
			return <-conn.directChan
		}

	}
}

func (conn *Connection) GetRPC() (client *rpc.Client) {
	select {
	case client = <-conn.rpcChan:
		return client
	default:
		//When too many clients call simultaenously
		conn.mu.Lock()
		if conn.rpcCnt < MaxTCPPerConn {
			rwc := conn.dialWithRetry()
			client = conn.makeClient(rwc)
			conn.rpcCnt++
			conn.mu.Unlock()
			return client
		} else {
			conn.mu.Unlock()
			return <-conn.rpcChan
		}

	}
}


func (conn *Connection) returnRPC(client *rpc.Client) {
	select {
	case conn.rpcChan <- client:
		return
	default:
		conn.mu.Lock()
		conn.rpcCnt--
		conn.mu.Unlock()
		client.Close()
	}
}

func (conn *Connection) ReturnDirect(rwc io.ReadWriteCloser) {
	select {
	case conn.directChan <- rwc:
		return
	default:
		conn.mu.Lock()
		conn.directCnt--
		conn.mu.Unlock()
		rwc.Close()
	}
}

func PortAvailable(port uint16) bool {
	ln, err := net.Listen("tcp", ":" + strconv.Itoa(int(port)))

	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

func GetOutboundIP(ip string) net.IP {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:22",ip))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return localAddr.IP
}


func (conn *Connection) Call(method string, args interface{}, reply interface{}) error {

	for {
		client := conn.GetRPC()

		ping := make(chan *rpc.Call, 1)
		done := make(chan *rpc.Call, 1)

		var a, b int
		client.Go(method, args, reply, done)


		var err error
		hit := false

		select {
		case res := <-done:
			err = res.Error
			hit = true
		case <-time.After(pingInterval):
			client.Go("ConnectionPingReplyer.Heartbeat", &a, &b, ping)
			log.Warnln("Not responding, begin to ping.")
		}

		for !hit {
			select {
			case res := <-done:
				err = res.Error
				hit = true
			case res := <-ping:
				if res.Error != nil {
					err = res.Error
					hit = true
				} else {
					go func() {
						time.Sleep(pingInterval)
						client.Go("ConnectionPingReplyer.Heartbeat", &a, &b, ping)
					}()
				}
			case <-time.After(pingTimeout):
				err = ErrTimeOut
				hit = true
				log.Warn("Ping Timeout for ", method, " ", conn.address.Load())
			}
		}
		var oserr syscall.Errno
		if err != rpc.ErrShutdown && err != ErrUnrecognizedMagic && err != ErrTimeOut && err != io.ErrUnexpectedEOF && err != io.EOF && !errors.As(err,&oserr){
			conn.returnRPC(client)
			return err
		} else {
			log.Errorln(conn.address.Load(),err)
			client.Close()
			conn.rpcChan <- conn.makeClient(conn.dialWithRetry())
		}

		log.Info("RPC reconnecting")
		time.Sleep(rpcReconnectTimeout)
	}

	return nil
}

//TODO: Should be implemented later.
func (conn *Connection) Close() {
	close(conn.directChan)
	close(conn.rpcChan)
	for direct := range conn.directChan {
		err := direct.Close()
		if err != nil{
			log.Errorln(err)
		}
	}
	for client := range conn.rpcChan {
		err := client.Close()
		if err != nil{
			log.Errorln(err)
		}
	}
}

func (conn *Connection) dialWithRetry() io.ReadWriteCloser{
	addr := conn.address.Load().(string)
	rwc, err := net.Dial("tcp", addr)
	if err != nil{
		log.Warnln("Error make connection for", addr, err, "retrying")
	}
	for err != nil {
		time.Sleep(rpcReconnectTimeout)
		addr = conn.address.Load().(string)
		rwc, err = net.Dial("tcp", addr)
	}
	return rwc
}

func (conn *Connection) makeClient(rwc io.ReadWriteCloser) *rpc.Client {

	n,err := rwc.Write([]byte{RPCByte})
	if n!=1 || err != nil {
		log.Errorln(err)
	}
	encBuf := bufio.NewWriterSize(rwc, 1<<20)
	reader := bufio.NewReaderSize(rwc, 1<<20)
	client := &IOBufClientCodec{
		rwc:       rwc,
		reader:    reader,
		iobufPool: conn.pool,
		dec:       gob.NewDecoder(reader),
		enc:       gob.NewEncoder(encBuf),
		encBuf:    encBuf,
	}
	return rpc.NewClientWithCodec(client)

}



func MakeConnectionWithIOPool(addr string, pool *IOBufferPool) *Connection {
	conn := &Connection{}
	conn.address.Store(addr)
	conn.mu = sync.Mutex{}
	conn.pool = pool
	conn.directChan = make(chan io.ReadWriteCloser, MaxTCPPerConn)
	conn.rpcChan = make(chan *rpc.Client, MaxTCPPerConn)


	conn.rpcChan <- conn.makeClient(conn.dialWithRetry())
	conn.rpcCnt = 1

	conn.directCnt = 0

	return conn
}
func MakeConnection(addr string) *Connection {
	return MakeConnectionWithIOPool(addr, MakeIOBufferPool())
}

func StartRPCServer(address string) (*rpc.Server,*net.TCPAddr,error) {
	rpcs := rpc.NewServer()

	rpcs.RegisterName("ConnectionPingReplyer", &ConnectionPingReplyer{})
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		log.Fatal(err)
		return rpcs, addr,err
	}

	return rpcs, addr,err
}

func ServeRPCServer(rpcs *rpc.Server,listener *net.TCPListener,directHandler func(conn net.Conn)) {
	//TODO: Memory should be limited here.

	pool := MakeIOBufferPool()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorln(err)
			continue
		}

		var byt [1]byte
		n, err := io.ReadFull(conn,byt[:])
		if n!= 1 || err != nil {
			log.Errorln(err)
		}

		byte := byt[0]

		if byte == RPCByte {
			buf := bufio.NewWriterSize(conn, 1<<20)
			reader := bufio.NewReaderSize(conn, 1<<20)
			srv := &IOBufServerCodec{
				rwc:    conn,
				reader: reader,
				pool:   pool,
				dec:    gob.NewDecoder(reader),
				enc:    gob.NewEncoder(buf),
				encBuf: buf,
			}
			go rpcs.ServeCodec(srv, conn.RemoteAddr())
		} else if byte == DirectByte {
			log.Infoln("Direct connection established.")
			go directHandler(conn)
		} else {
			log.Errorln("Unrecognized byte",byte,conn.LocalAddr(),conn.RemoteAddr())
		}
	}
}

//*/
