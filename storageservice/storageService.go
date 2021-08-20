package stor

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"rcstor/common"
	"sync"
	"time"
)


const (
	GetBatch byte = 0
	GetNext byte = 1
	Put byte = 2
	WriteFinished byte = 3
)

type StorageService struct {
	UUID      uuid.UUID
	WorkSpace string
	Files     map[string]*os.File
	mu        sync.Mutex
	pool      *common.IOBufferPool

	serializer sync.Mutex
	IsSSD		bool

}


func MakeStorageService(workSpace string) *StorageService {
	service := &StorageService{WorkSpace: workSpace}

	service.Files = make(map[string]*os.File)
	if _, err := os.Stat(service.WorkSpace); os.IsNotExist(err) {
		os.MkdirAll(service.WorkSpace, 0775)
	}

	uuidPath := filepath.Join(service.WorkSpace, "uuid")
	if _, err := os.Stat(uuidPath); os.IsNotExist(err) {
		file, _ := os.Create(uuidPath)
		defer file.Close()
		service.UUID = uuid.NewV1()
		file.Write(service.UUID.Bytes())
	} else {
		file, _ := os.Open(uuidPath)
		defer file.Close()
		bytes, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalln(err)
		}
		service.UUID, err = uuid.FromBytes(bytes)
		if err != nil {
			log.Fatalln("Invalid uuid", err)
		}
	}
	service.pool = common.MakeIOBufferPool()
	return service
}

type byteReader struct {
	io.Reader
}

// New returns an io.ByteReader from an io.byteReader.
// If r is already a ByteReader, it is returned.
func NewByteReader(r io.Reader) *byteReader {
	return &byteReader{r}
}

func (r *byteReader) ReadByte() (byte, error) {
	var buf [1]byte
	n, err := r.Reader.Read(buf[:])
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, io.ErrNoProgress
	}
	return buf[0], nil
}

func (service *StorageService) HandleDirect(rwc net.Conn) {

	//Maximal 1GB buffer
	log.Println("Begin to handle direct connection.")

	for {
		var opcode [1]byte
		n,err := rwc.Read(opcode[:])
		if err != nil || n!=1 {
			if err != io.EOF {
				log.Errorln("Unable to fetch protocol code", err)
			}
			rwc.Close()
			return
		}
		if opcode[0] == GetBatch {

			log.Infoln("Begin to handle Get")
			decoder := gob.NewDecoder(NewByteReader(rwc))
			var reply GetBatchDataArgs
			err = decoder.Decode(&reply)
			if err != nil {
				if err == io.EOF {
					return
				} else {
					log.Errorln(err)
					rwc.Close()
					return
				}
			} else {

				ch := make(chan common.IOBuffer,4096)
				finished := make(chan bool,1)
				go func(){
					if reply.NetSizes != nil {
						for i:=0;i<len(reply.NetSizes);i++{
							//OPCode may be swallowed by gob
							var opcode [1]byte
							n,err := rwc.Read(opcode[:])
							if err != nil || n!=1 {
								if err != io.EOF {
									log.Errorln("Unable to fetch protocol code", err)
								}
								rwc.Close()
								finished<-true
								return
							}
							if opcode[0] != GetNext {
								log.Errorln("Unrecogonized opcode, should be getnext",opcode[0])
								rwc.Close()
								finished<-true
								return
							}
							start := time.Now()
							size := reply.NetSizes[i]
							written := uint32(0)
							for written < size {
								buffer := <-ch
								tried := 0
								for tried < len(buffer.Data) {
									begin := time.Now()
									rwc.SetWriteDeadline(begin.Add(common.IOTimeout))
									n,err = rwc.Write(buffer.Data[tried:])
									tried += n
									if err, ok := err.(net.Error); ok && err.Timeout() {
										log.Errorln(n,err)
									} else if err != nil {
										log.Errorln(n,err)
										rwc.Close()
										finished<-true
										return
									}
								}

								buffer.Unref()
								written += uint32(n)
							}
							log.Infoln("Time consumed on sending",reply.NetSizes[i],time.Since(start))

						}
						rwc.SetWriteDeadline(time.Time{})
					} else {
						for buffer := range ch {
							n, err := rwc.Write(buffer.Data)
							if err != nil || n != len(buffer.Data) {
								log.Errorln(n, err)
							}
							buffer.Unref()
						}
					}
					finished<-true
				}()
				start := time.Now()
				service.getBatchDataPipelined(&reply, ch)
				close(ch)
				log.Infoln("Time consumed on disk read",time.Since(start))
				<-finished
				log.Infoln("Time consumed on network",time.Since(start))
			}
		} else if opcode[0] == Put {

			var reply PutDataArgs

			var arg [22]byte
			n,err = rwc.Read(arg[:])

			if n!=22 || err != nil {
				if err == io.EOF {
					return
				} else {
					log.Errorln(err)
					rwc.Close()
					return
				}
			} else {
				reply.BlockId = int16(binary.LittleEndian.Uint16(arg[:]))
				reply.Offset = binary.LittleEndian.Uint64(arg[2:])
				reply.Len = binary.LittleEndian.Uint64(arg[10:])
				reply.PGId = binary.LittleEndian.Uint32(arg[18:])


				log.Infoln("Begin to handle Put", reply)
				service.PutDataPipelined(&reply, rwc)
			}
		} else {
			log.Errorln("Unrecogonized opcode",opcode[0])
			rwc.Close()
			return
		}
	}
}

func (service *StorageService) getDataPipelined(args *GetDataArgs, net chan common.IOBuffer) {
	if !service.IsSSD {
		service.serializer.Lock()
		defer service.serializer.Unlock()
	}
	start := time.Now()
	fd := service.getFd(args.PGId, args.BlockId)

	for off:=uint64(0);off<uint64(args.Size); off+=common.StorReadMinSize {
		size := int(common.Min(common.StorReadMinSize,uint64(args.Size) - off))
		buffer := service.pool.GetBuffer(size)
		_, err := fd.ReadAt(buffer.Data, int64(args.Offset + off))
		if err != nil {
			log.Errorln(err)
		}

		net <-buffer
	}
	log.Println("Get data with size", args.Size, "time consumed:", time.Since(start))

}

func (service *StorageService) getBatchDataPipelined(args *GetBatchDataArgs, net chan common.IOBuffer)  {
	if !service.IsSSD {
		service.serializer.Lock()
		defer service.serializer.Unlock()
	}
	start := time.Now()
	Compress(args)
	totalSize := uint32(0)
	for _, size := range args.Size {
		totalSize += size
	}

	if len(args.BlockId) != len(args.Size) || len(args.BlockId) != len(args.Offset) {
		panic("GetBatchData exception")
	}

	for i := range args.BlockId {
		fd := service.getFd(uint32(args.PGId), args.BlockId[i])
		size := args.Size[i]
		for off:=uint64(0);off<uint64(size); off+=common.StorReadMinSize {
			size := int(common.Min(common.StorReadMinSize,uint64(size) - off))
			buffer := service.pool.GetBuffer(size)

			n, err := fd.ReadAt(buffer.Data, int64(args.Offset[i] + off))
			if err != nil && err != io.EOF{
				log.Errorln(err)
			}
			if n < size {
				copy(buffer.Data[n:],bytes.Repeat([]byte{0x00},size -n))
			}
			net <-buffer
		}
	}

	//log.Infoln("PGId:",args.PGId,"Get batch size=", totalSize, " offset=", args.Offset[0], " frag=", len(args.Offset), "time consumed:", time.Since(start))
	log.Infoln("Get batch data, size =",totalSize,", start=",start.UnixNano()/1e6," end=",time.Now().UnixNano()/1e6)
	//log.Println("Finish get:", time.Now())
}


func (service *StorageService) GetData(args *GetDataArgs, reply *common.IOBuffer) error {
	//mutex
	if !service.IsSSD {
		service.serializer.Lock()
		defer service.serializer.Unlock()
	}
	start := time.Now()
	fd := service.getFd(args.PGId, args.BlockId)

	*reply = service.pool.GetBuffer(int(args.Size))
	n, err := fd.ReadAt(reply.Data, int64(args.Offset))
	//log.Infoln(args.Size, n, err)
	if n > int(args.Size) {
		n = int(args.Size)
	}
	reply.SetBytesToSend(n)

	log.Println("Get data with size", args.Size, "time consumed:", time.Since(start), " ", n, " ", err)
	return nil
}
func Compress(args *GetBatchDataArgs) {
	if len(args.BlockId) == 0 {
		return
	}
	l := 0
	for i := 1; i < len(args.BlockId); i++ {
		if args.BlockId[i] == args.BlockId[l] && args.Offset[i] == args.Offset[l]+uint64(args.Size[l]) {
			args.Size[l] += args.Size[i]
		} else {
			l++
			args.BlockId[l] = args.BlockId[i]
			args.Offset[l] = args.Offset[i]
			args.Size[l] = args.Size[i]
		}
	}
	l++
	args.BlockId = args.BlockId[:l]
	args.Offset = args.Offset[:l]
	args.Size = args.Size[:l]
}


func (service *StorageService) GetBatchData(args *GetBatchDataArgs, reply *common.IOBuffer) error {
	if !service.IsSSD {
		service.serializer.Lock()
		defer service.serializer.Unlock()
	}
	start := time.Now()
	Compress(args)
	totalSize := uint32(0)
	for _, size := range args.Size {
		totalSize += size
	}
	if totalSize%common.StorageOffsetAlign == 0 {
		*reply = service.pool.GetBuffer(int(totalSize))
	} else {
		*reply = service.pool.GetBuffer(int(totalSize + common.StorageOffsetAlign - totalSize%common.StorageOffsetAlign))
	}
	if len(args.BlockId) != len(args.Size) || len(args.BlockId) != len(args.Offset) {
		panic("GetBatchData exception")
	}
	offset := uint64(0)
	reply.SetBytesToSend(0)

	for i := range args.BlockId {
		fd := service.getFd(uint32(args.PGId), args.BlockId[i])
		size := args.Size[i]
		if size%common.StorageOffsetAlign != 0 {
			size += common.StorageOffsetAlign - size%common.StorageOffsetAlign
		}
		n, _ := fd.ReadAt((reply.Data)[offset:offset+uint64(size)], int64(args.Offset[i]))
		if n > 0 {
			reply.SetBytesToSend(int(offset) + n)
		}
		offset += uint64(args.Size[i])
	}

	log.Infoln("PGID=",args.PGId,"Get batch size=", totalSize, " offset=", args.Offset[0], " frag=", len(args.Offset), "time consumed:", time.Since(start))

	//log.Println("Finish get:", time.Now())

	return nil
}

func (service *StorageService) GetBlockSize(args *GetBlockSizeArgs, reply *uint64) error {
	fd := service.getFd(args.PGId, args.BlockId)
	info, err := fd.Stat()
	if err != nil {
		log.Error(err.Error(), " GetBlockSize ", args.BlockId, " ", args.PGId)
		log.Error(args.PGId, args.BlockId)
		*reply = 0
	} else {
		*reply = uint64(info.Size())
	}
	return nil
}

func  (service *StorageService) PutDataPipelined(args *PutDataArgs,rwc io.ReadWriteCloser)  {

	fd := service.getFd(args.PGId, args.BlockId)

	//256M memory per connection, with 16 max connection.
	ch := make(chan common.IOBuffer,2048)
	go func(rwc io.ReadWriteCloser) {
		for off:=uint64(0);off<args.Len;off+=common.StorWriteMinSize {
			size := int(common.Min(common.StorWriteMinSize,args.Len - off))
			buf := service.pool.GetBuffer(size)
			n,err := io.ReadFull(rwc,buf.Data)
			if err != nil || n!=size {
				log.Errorln(n,err)
				rwc.Close()
				return
			}
			ch <- buf
		}
	}(rwc)


	if !service.IsSSD {
		service.serializer.Lock()
		defer service.serializer.Unlock()
	}
	start := time.Now()
	for off:=uint64(0);off<args.Len;off+=common.StorWriteMinSize {
		buf := <-ch

		n,err := fd.WriteAt(buf.Data,int64(off+args.Offset))
		if n != len(buf.Data) || err != nil{
			log.Errorln(err)
			rwc.Close()
			buf.Unref()
			return
		}
		buf.Unref()
	}
	close(ch)
	log.Infoln("Put data, size =",args.Len,", start=",start.UnixNano()/1e6," end=",time.Now().UnixNano()/1e6)
	//fd.Sync()
	rwc.Write([]byte{WriteFinished})
}

func (service *StorageService) getFd(PGId uint32, blockId int16) *os.File {
	service.mu.Lock()
	defer service.mu.Unlock()
	if blockId < 0 {
		blockId = 0
	}
	dirPath := filepath.Join(service.WorkSpace, fmt.Sprintf("PG-%d/", PGId))
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		os.MkdirAll(dirPath, 0775)
	}
	filename := filepath.Join(dirPath, fmt.Sprintf("Block-%d", blockId))
	fd, ok := service.Files[filename]
	if !ok {
		fd, _ = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		service.Files[filename] = fd
	}
	return fd
}
