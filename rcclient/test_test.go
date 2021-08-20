package rcclient

import (
	"bufio"
	"bytes"
	"github.com/sirupsen/logrus"
	"io"
	"math"
	"math/rand"
	"os"
	"rcstor/common"
	"rcstor/dir"
	index "rcstor/indexservice"
	"strconv"
	"sync"
	"testing"
	"time"
)

type PG struct{
	diskBlocks [][]uint64
	totalSize uint64
}

func init(){
	logrus.SetReportCaller(true)
}

func TestLimiter(t *testing.T) {
	byte := bytes.Repeat([]byte{0},1<<20 * 1000)

	src := bytes.NewReader(byte)

	// Destination
	file,err := os.OpenFile("/dev/null",os.O_RDWR|os.O_CREATE,0644)
	if err != nil{
		t.Error(err)
	}
	writer := RateWriter(file,common.MaxClientBandwidth)
	start := time.Now()
	io.Copy(writer,src)
	logrus.Infoln(time.Since(start))
}

func TestTraceSampleGet(t *testing.T) {
	trace := GetTrace(common.SSDMaxSize,common.HDDMaxSize,common.HDDNumObjects)
	ch := trace.SampleGet()
	sizes := trace.SampleSizes()
	avg := 0.0
	for _,size := range sizes {
		avg += float64(size)
	}
	avg = avg / float64(len(sizes))

	total := 0.0
	sampleNum := int(common.HDDNumObjects) / 1000 *1000
	for i:=0;i<sampleNum;i++{
		oid := <-ch
		total += float64(sizes[oid])
	}
	logrus.Println("Average get size:",total/float64(sampleNum)/(1<<20),"MB")
	logrus.Println("Average size:",avg/(1<<20), "MB")
}

func TestTraceStripe(t *testing.T) {
	trace := GetTrace(0,common.SSDMaxSize,common.SSDNumObjects)

	sizes := trace.SampleSizes()
	total := uint64(0)
	cap := uint64(0)
	for _,size := range sizes {
		total += size
		k := 10
		blockSize := uint64(256 << 10)
		left := size % (blockSize * uint64(k))
		minSize := uint64(256 * k)

		padding := minSize - left%minSize
		cap += (size + padding)
	}

	logrus.Println("Total size:",float64(total) * 1.4 / 16.0 /(1<<30) ,"GB")
	logrus.Println("Cap size:",float64(cap) * 1.4 / 16.0 /(1<<30), "GB")
}
func TestGeometric(t *testing.T) {
	trace := GetTrace(0,common.SSDMaxSize,common.SSDNumObjects)

	MaxQ := 10

	blocks := make([]int, MaxQ)

	for i:=0;i<MaxQ;i++ {
		p := &index.GeometricPartitioner{MinBlock: 128<<10,MaxBlock: 1<<30,Base: i + 1}
		blocks[i] = 0
		totalSize := uint64(0)
		sizes := trace.SampleSizes()
		for _,size := range sizes{
			blocks[i] += len(p.Split(uint64(size))) - 1
			totalSize+=size
		}
		println(totalSize  /  uint64(blocks[i]))
	}
}

func TestPartition(t *testing.T) {
	//96 disks

	nPG := 200
	trace := GetTrace(4<<20,4<<30,uint64(170000/nPG))

	partitioner := index.GeometricPartitioner{MinBlock: 4<<20,MaxBlock: 256<<20,Base: 2}
	PGS := make([]PG,nPG)
	k:= 10

	for i:=0;i<nPG;i++{
		PGS[i].diskBlocks = make([][]uint64,k)
	}

	sizes := trace.SampleSizes()

	diskBlocks := make([][]uint64,k)
	for i:=0;i<k;i++{
		diskBlocks[i] = make([]uint64,12)
	}
	total := 0.0
	for _,size := range sizes {
		total += float64(size)
		splits := partitioner.Split(uint64(size))

			blockID := partitioner.SizeToBlockId(splits[len(splits) - 1])
			diskId := 0
			for j:=0;j<k;j++ {
				if diskBlocks[j][blockID] < diskBlocks[diskId][blockID] {
					diskId = j
				}
			}
			for i:=0;i<len(splits);i++ {
				diskBlocks[diskId][partitioner.SizeToBlockId(splits[i])] += splits[i]
			}
	}
	totalParied := 0.0
	for block:=0;block<12;block++ {
		diskId := 0
		for i:=0;i<k;i++ {
			if diskBlocks[i][block] > diskBlocks[diskId][block] {
				diskId = i
			}
		}
		totalParied += float64(diskBlocks[diskId][block]) * 4
	}

	logrus.Println(diskBlocks)
	logrus.Println((totalParied+total)/total)

}

func TestContiguousService(t *testing.T) {
	var para dir.VolumeParameter
	para.NumberPG = 10
	para.Layout = common.Contiguous
	para.K = 10
	para.Redundancy = 4
	para.BlockSize = (1<<20)
	testService("Contiguous-1M",para)
}
func TestStripeService(t  *testing.T) {
	var para dir.VolumeParameter
	para.NumberPG = 10
	para.Layout = common.Stripe
	para.K = 10
	para.Redundancy = 4
	para.BlockSize = (1<<18)
	para.IsSSD = true
	testService("Stripe-256K",para)
}

func TestGeometricService(t *testing.T) {
	var para dir.VolumeParameter
	para.NumberPG = 10
	para.Layout = common.Geometric
	para.K = 10
	para.Redundancy = 4
	para.MinBlockSize = (1<<20)
	para.MaxBlockSize = (256<<20)
	para.GeometricBase = 2

	testService("Geometric-1M",para)
}


func testService(volumeName string, para dir.VolumeParameter) {

	service :=  dir.StartDirectoryService("/tmp/db",30100)
	servers := make([]dir.ServerInfo,1)
	servers[0].IP="localhost"
	servers[0].Dir = make([]string,14)
	for i:=0;i<14;i++ {
		servers[0].Dir[i] = "/Users/syd/disks/brick"+strconv.Itoa(i)
	}

	args := dir.CreateVolumeArgs{VolumeName: volumeName,Servers: servers,Parameter: para}

	err := service.CreateVolume(&args,nil)
	//logrus.Println(service.Volumes.Load(volumeName))
	if err != nil {
		logrus.Errorln(err)
	}
	err = service.StartVolume(&args.VolumeName,nil)
	if err != nil {
		logrus.Errorln(err)
	}

	pool := common.MakeIOBufferPool()
	rand.Seed(0)

	iterNum := 200
	sizes := make([]int,iterNum)
	data := make([]common.IOBuffer,iterNum)
	for i:=0;i<iterNum;i++ {
		sizes[i] = rand.Intn(4 * (1 << 20))

		data[i] = pool.GetBuffer(sizes[i])
		rand.Read(data[i].Data)
	}

	client := MakeClient("0.0.0.0:30100", volumeName)

	logrus.Println("Begin to put objects.",client.Pool)
	var wg sync.WaitGroup
	for i := 0; i < iterNum; i++ {
		wg.Add(1)
		func(i int) {
			objectID := uint64(i + 1)
			logrus.Println("Puting",objectID,"size:",len(data[i].Data))
			client.Put(objectID, data[i])
			wg.Done()
		}(i)

	}
	wg.Wait()


	logrus.Println("Begin to test get.")
	for objectID:=uint64(1);objectID<=uint64(iterNum);objectID++ {
		var b bytes.Buffer
		writer := bufio.NewWriter(&b)
		client.Get(writer,objectID,math.MaxUint32,0)
		writer.Flush()

		if !bytes.Equal(b.Bytes(), data[objectID-1].Data) {
			for i:=0;i<b.Len();i++{
				if b.Bytes()[i] != data[objectID - 1].Data[i] {
					logrus.Errorln(i,b.Bytes()[i],data[objectID - 1].Data[i])
					break
				}
			}
			logrus.Fatalln("Data not equal for object",objectID,b.Len(),len(data[objectID -1].Data))
		}
	}

	logrus.Println("...Passed")

	err = service.StopVolume(&args.VolumeName,nil)
	if err != nil {
		logrus.Errorln(err)
	}

	err = service.StartVolume(&args.VolumeName,nil)
	if err != nil {
		logrus.Errorln(err)
	}

	logrus.Println("Begin to generate parity.")

	wg.Add(para.NumberPG)
	for i:=0;i<para.NumberPG;i++ {
		func(pgId int){
			client.GeneratePGParity(pgId)
			wg.Done()
		}(i)
	}
	wg.Wait()

	logrus.Println("Begin degraded reading.")

	for objectID:=uint64(1);objectID<=uint64(iterNum);objectID++ {
		var b bytes.Buffer
		writer := bufio.NewWriter(&b)
		client.DGet(writer,objectID,math.MaxUint32,0)
		writer.Flush()

		if !bytes.Equal(b.Bytes(), data[objectID-1].Data) {
			for i:=0;i<b.Len();i++{
				if b.Bytes()[i] != data[objectID - 1].Data[i] {
					logrus.Errorln(i,b.Bytes()[i],data[objectID - 1].Data[i])
					break
				}
			}
			logrus.Errorln("Data not equal for object",objectID,b.Len(),len(data[objectID -1].Data))
		}
	}


	err = service.StopVolume(&args.VolumeName,nil)
	if err != nil {
		logrus.Errorln(err)
	}
	service.Close()


}
/*
func Recovery(client *Client) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	mu.Lock()
	mu.Unlock()
	for i := 0; i < common.NLV; i++ {
		wg.Add(1)
		go func(lvID int) {
			log.Println("Begin recovery for", lvID)
			var lv dir.PlacementGroup
			client.DirConn.Call("DirectoryService.GetLV", &lvID, &lv, time.Second*30)
			broken := 0
			var PV dir.Brick
			PV.ServerIP = "127.0.0.1"
			PV.ServerPV = uint16(100 + lvID)
			PV.ServerPort = lv.Bricks[broken].ServerPort
			client.EC.Recovery(lv, broken, PV)
			log.Println("Finish recovery for", lvID)
			//println("broken:", lv.Bricks[broken].ServerPV, lv.Bricks[broken].ServerPort)

			PVNUM := PV.ServerPort - 33100

			log.Infoln("Begin to compare results.")
			path1 := fmt.Sprintf("./stor/%d/localhost-%d-%d/", PVNUM, PV.ServerPort, lv.Bricks[broken].ServerPV)
			path2 := fmt.Sprintf("./stor/%d/127.0.0.1-%d-%d/", PVNUM, PV.ServerPort, PV.ServerPV)
			cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("diff -r %s %s", path1, path2))
			for i := 0; i <= 8; i++ {
				exec.Command("/bin/sh", "-c", fmt.Sprintf("touch %s%d", path1, i)).Run()
				exec.Command("/bin/sh", "-c", fmt.Sprintf("touch %s%d", path2, i)).Run()
			}
			log.Infoln("Compare finished for", lvID)

			if s, err := cmd.Output(); err != nil {
				println("diff -r", path1, path2)
				println("out=", len(s))
				panic(err)
			}

			wg.Done()
		}(i)
	}
	wg.Wait()
}
*/