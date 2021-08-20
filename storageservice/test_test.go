package stor

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"math/rand"
	_ "os/exec"
	"rcstor/common"
	_ "rcstor/indexservice"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	service := MakeStorageService("./test1")
	pool := common.MakeIOBufferPool()
	rand.Seed(time.Now().UnixNano())
	data := pool.GetBuffer(rand.Intn(1<<24))
	n,err := rand.Read(data.Data)
	if err != nil {
		logrus.Fatalln(err)
	}
	//{
	//	var args PutDataArgs
	//	args.BlockId = 0
	//	args.Data = data
	//	args.Offset = 0
	//	args.PGId = 0
	//	service.PutData(&args, nil)
	//	args.Data.Unref()
	//}
	{
		var args GetDataArgs
		var reply common.IOBuffer
		args.BlockId = 0
		args.Size = uint32(n)
		args.Offset = 0
		args.PGId = 0

		service.GetData(&args, &reply)
		if bytes.Compare(reply.Data,data.Data) != 0 {
			logrus.Debugln(reply.Data[:10],len(reply.Data))
			logrus.Debugln(data.Data[:10],len(data.Data))
			logrus.Fatalln("Data invalid")
		}
	}
}