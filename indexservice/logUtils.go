package index

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"path/filepath"
)

const CheckPointInterval = 1000


var ErrExist = errors.New("index existed")

type PutIndexArgsOnlyPGId struct {
	ObjectID uint64
	Size     uint64
	PGId     uint32
}

func AddLog(args *PutIndexArgs, dirPath string, IndexNum uint64, data interface{}, logFdTable map[uint32]*os.File) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		os.MkdirAll(dirPath, 0775)
	}
	if IndexNum%CheckPointInterval == 0 {
		file, _ := os.OpenFile(filepath.Join(dirPath, "new.dat"), os.O_CREATE|os.O_RDWR, 0666)
		encoder := gob.NewEncoder(file)
		encoder.Encode(data)
		file.Close()
		cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("cd %s && mv new.dat chkpt.dat && rm -rf log.dat", dirPath))
		cmd.Run()
		delete(logFdTable, args.PGId)
		return
	}
	fd, ok := logFdTable[args.PGId]
	if !ok {
		fd, _ = os.OpenFile(filepath.Join(dirPath, "log.dat"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		logFdTable[args.PGId] = fd
	}
	logData := ToBytes(args)
	n,err := fd.Write(logData)
	if n != len(logData) || err != nil {
		log.Errorln(err)
	}
	//if IndexNum % (CheckPointInterval / 10) == 0 {
	//	fd.Sync()
	//}
	fd.Sync()
}

func Recovery(dirPath string, data interface{}, service IndexServiceInterface) {
	if _, err := os.Stat(filepath.Join(dirPath, "chkpt.dat")); err == nil || os.IsExist(err) {
		file, _ := os.Open(filepath.Join(dirPath, "chkpt.dat"))
		dec := gob.NewDecoder(file)
		dec.Decode(data)
		file.Close()
	}
	if _, err := os.Stat(filepath.Join(dirPath, "log.dat")); err == nil || os.IsExist(err) {
		file, _ := os.Open(filepath.Join(dirPath, "log.dat"))
		args := &PutIndexArgs{}
		reply := &ObjectIndex{}
		l, _ := file.Seek(0, os.SEEK_END)
		file.Seek(0, os.SEEK_SET)
		dataSize := binary.Size(args)
		buf := make([]byte, l)
		file.Read(buf)
		for i := 0; i < int(l); i += dataSize {
			BytesTo(buf[i:], args)
			service.PutIndex(args, reply)
		}
		file.Close()
	}
}

func DropFolder(dirPath string) {
	os.RemoveAll(dirPath)
}

func ToBytes(data interface{}) []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, data)
	return buf.Bytes()
}

func BytesTo(data []byte, reply interface{}) {
	bytesBuffer := bytes.NewBuffer(data)
	binary.Read(bytesBuffer, binary.BigEndian, reply)
}
