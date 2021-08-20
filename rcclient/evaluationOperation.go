package rcclient

import (
	"crypto/rand"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"math"
	"net/http"
	"os"
	"rcstor/common"
	"rcstor/dir"
	"sync"
	"time"
)


func readHttp(url string)  {
	resp,err := http.Get(url)
	if err != nil {
		logrus.Errorln(err)
	}
	defer resp.Body.Close()
	nullFile,_ := os.OpenFile("/dev/null",os.O_RDWR|os.O_CREATE,0644)
	defer nullFile.Close()
	io.Copy(nullFile,resp.Body)
}

func (client *Client) getTrace() *Trace{
	if client.Pool.GetVolume().Parameter.IsSSD {
		return GetTrace(0,common.SSDMaxSize,common.SSDNumObjects)
	} else {
		return GetTrace(common.SSDMaxSize,common.HDDMaxSize,common.HDDNumObjects)
	}
}

func (client *Client) TracePuts(args *interface{},reply *interface{}) error{

	logrus.Infoln("Begin to tracePuts, clientId:",client.ClientId)
	buffer := make([]byte,math.MaxUint32)
	rand.Read(buffer)
	logrus.Infoln("Data generated.")

	trace := client.getTrace()

	sizes := trace.SampleSizes()

	var wg sync.WaitGroup

	clients := 16
	ch := make(chan uint64)
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func() {
			for objectId := range ch {
				size := sizes[objectId]
				logrus.Println("Begin to put object", objectId, "size=", size)

				client.Put(objectId, common.IOBuffer{Data: buffer[0:size]})

				logrus.Println("Finish put object", objectId)
			}
			wg.Done()
		}()
	}

	for objectId := uint64(client.ClientId); objectId < trace.NumObjects; objectId += uint64(client.NumClients) {
		ch <- objectId
	}
	close(ch)
	wg.Wait()
	return nil
}

func (client *Client) GenerateParity(args* interface{},reply *interface{}) error{
	var wg sync.WaitGroup
	clients := 16
	ch := make(chan int)
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func() {
			for pgID := range ch {

				logrus.Println("Begin to generate parity for", pgID)
				client.GeneratePGParity(pgID)
				logrus.Println("Finish generate parity for", pgID)
			}
			wg.Done()
		}()
	}
	nPG := len(client.Pool.GetVolume().PGs)
	pgPerClient := (nPG + client.NumClients - 1) / client.NumClients

	for i := client.ClientId * pgPerClient; i < (client.ClientId +1) * pgPerClient && i < nPG; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
	return nil
}

func (client *Client) ForegroundTraceGets(args *interface{}, reply *interface{}) error{

	trace := client.getTrace()
	ch := trace.SampleGet()


	for i := 0; i < common.ForegroundClients; i++ {

		go func(clientId int) {
			for objectId := range ch {
				url := fmt.Sprintf("http://localhost:%d/get/%d", client.LocalHttpPort, objectId)
				readHttp(url)
				logrus.Println("Finish get clientId:", clientId, "objectId:", objectId)
			}
		}(i)
	}
	return nil
}
func (client *Client) GetRecoveryTasks(args *dir.MoveInstruct,reply *[]dir.RecoveryTask) error {
	*reply = client.EC.GetRecoverTasks(*args)
	return nil
}

func (client *Client) Recovery(args *interface{},reply *interface{}) error {
	logrus.Infoln("Begin to recovery")
	client.EC.Recovery()
	logrus.Infoln("Finish recovery")
	return nil
}

func (client *Client) TraceDget(args *interface{}, reply *interface{}) error {

	trace := client.getTrace()

	sample := trace.SampleGet()
	sizes := trace.SampleSizes()

	totalSize := uint64(0)
	totalLatency := 0.0

	sampleNum := int(float64(trace.NumObjects) / 1000)

	for i:=0;i<sampleNum;i++{
		oid := <-sample

		url := fmt.Sprintf("http://localhost:%d/dget/%d",client.LocalHttpPort, oid)

		logrus.Println("Begin to read objectId:", oid)
		start := time.Now()
		readHttp(url)
		latency := time.Since(start)

		logrus.Println("Finish read, latency:", latency)
		totalLatency += latency.Seconds()
		totalSize += sizes[oid]
	}
	logrus.Println("total latency:", totalLatency,"s.", "Total sizes:",totalSize >> 20,"MB.")
	logrus.Println("Average latency:",totalLatency/float64(sampleNum),"s.")
	return nil
}
