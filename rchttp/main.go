package main

import (
	"fmt"
	"github.com/facebookgo/freeport"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
	"github.com/urfave/cli"
	"io/ioutil"
	"log"
	"log/syslog"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/rcclient"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	GET = iota
	DGET
)

var client *rcclient.Client

var req_num = 0

var mu sync.Mutex

func Write(w http.ResponseWriter, r *http.Request, TypeGet int) {
	vars := mux.Vars(r)
	objectId, _ := strconv.ParseUint(vars["objectId"], 10, 64)

	offset := uint64(0)
	if str := r.URL.Query().Get("offset"); str != "" {
		offset, _ = strconv.ParseUint(str, 10, 64)
	}
	size := uint64(math.MaxUint64)
	if str := r.URL.Query().Get("size"); str != "" {
		size, _ = strconv.ParseUint(str, 10, 64)
	}
	mu.Lock()
	req_num++
	//logrus.Infoln(objectId, offset, size, req_num)
	mu.Unlock()

	w.WriteHeader(http.StatusOK)
	switch TypeGet {
	case GET:
		client.Get(w, objectId, size, offset)
		break
	case DGET:
		client.DGet(w, objectId, size, offset)
		break
	}
}

func DgetHandler(w http.ResponseWriter, r *http.Request) {
	Write(w, r, DGET)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	Write(w, r, GET)
}

func PutHandle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	objectId, _ := strconv.ParseUint(vars["objectId"], 10, 64)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
	}

	client.Put(objectId, common.IOBuffer{Data: body})
	w.WriteHeader(http.StatusOK)

}

func main() {

	app := cli.NewApp()
	app.Name = "RCHttp"
	app.Description = "HTTP server for rcstor."

	var volumeName string
	var dirAddr string
	var port int

	port,_ = freeport.Get()

	app.Version = "0.1"
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "volume",
			Value:       "",
			Usage:       "The volume name",
			Destination: &volumeName,
		},
		cli.StringFlag{
			Name:        "dirAddr",
			Value:       "127.0.0.1:30100",
			Usage:       "The address of directory service.",
			Destination: &dirAddr,
		},
	}

	app.Action = func(c *cli.Context) error {

		logDir := common.LogDIR
		logPath := filepath.Join(logDir,volumeName,"http.log")
		os.MkdirAll(filepath.Join(logDir,volumeName),0777)
		file,err := os.OpenFile(logPath,os.O_CREATE|os.O_WRONLY|os.O_APPEND,0644)
		if err != nil {
			logrus.Fatal(err)
		}
		//To handle panic
		syscall.Dup2(int(file.Fd()), 2)
		logrus.SetOutput(file)
		logrus.SetReportCaller(true)

		IP := dirAddr[:strings.Index(dirAddr,":")]
		hook, err := logrus_syslog.NewSyslogHook("udp",fmt.Sprintf("%s:514",IP),syslog.LOG_LOCAL2,"")
		if err != nil {
			logrus.Error("Unable to connect to local syslog daemon")
		} else {
			logrus.AddHook(hook)
		}


		lockFileName := filepath.Join("/tmp/rchttp-"+volumeName+"-lock")
		lock, e := os.Create(lockFileName)
		if e != nil {
			logrus.Fatalf("Failed to create log: %s", e)
		}
		defer os.Remove(lockFileName)
		defer lock.Close()

		e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e != nil {
			logrus.Fatalf("Failed to get lock: %s", e)
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

		addr := "0.0.0.0:0"
		s, l, _:= common.StartRPCServer(addr)


		listener, err := net.ListenTCP("tcp", l)
		if err != nil {
			log.Fatalln(err)
		}


		conn := common.MakeConnection(dirAddr)

		var volume dir.Volume

		err = conn.Call("DirectoryService.GetVolume",&volumeName,&volume)
		if err != nil {
			logrus.Fatalln(err)
		}
		port = volume.Parameter.HTTPPort

		var args dir.HttpRegisterArgs
		var reply dir.HttpRegisterReply

		args.Port = uint16(port)
		args.PID = os.Getpid()
		args.VolumeName = volumeName
		args.ClientPort = uint16(listener.Addr().(*net.TCPAddr).Port)

		err = conn.Call("DirectoryService.HttpRegister",&args,&reply)
		//logrus.Infoln("Finish registering HTTP.")
		if err != nil {
			logrus.Errorln(err)
		}

		go func() {
			var reply dir.HttpRegisterReply
			for {
				time.Sleep(common.HeartBeatInterval)
				err = conn.Call("DirectoryService.HttpRegister",&args,&reply)
				//logrus.Infoln("Finish registering HTTP.")
				if err != nil {
					logrus.Errorln(err)
				}
			}
		}()

		client = rcclient.MakeClientWithConn(conn,volumeName)
		client.ClientId = reply.ClientId
		client.NumClients = reply.NumClients
		client.LocalHttpPort = uint16(port)

		s.RegisterName("Client", client)
		go common.ServeRPCServer(s, listener,nil)

		router := mux.NewRouter().StrictSlash(true)
		router.HandleFunc("/get/{objectId}", GetHandler)
		router.HandleFunc("/dget/{objectId}", DgetHandler)
		router.HandleFunc("/put/{objectId}", PutHandle)
		err = http.ListenAndServe("0.0.0.0:"+strconv.Itoa(int(port)), router)
		if err != nil {
			logrus.Fatal("ListenAndServe: ", err)
		}

		return nil
	}

	app.Run(os.Args)
}
