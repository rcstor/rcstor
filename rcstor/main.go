package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
	"github.com/urfave/cli"
	"log"
	"log/syslog"
	"os"
	"path/filepath"
	"rcstor/common"
	"strings"
	"syscall"
)


func main() {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("Panic: %v", r)
			os.Exit(1)
		}
	}()

	app := cli.NewApp()
	app.Name = "rcstor"
	app.Description = "Storage Server for rcstor"

	var workspace string
	var volumeName string
	var dirAddr string

	app.Version = "0.1"
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "workspace",
			Value:       "./rcstor",
			Usage:       "The folder to place all storage data",
			Destination: &workspace,
		},
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
		name := workspace
		if name[len(workspace) -1 ] == '/' {
			name = workspace[:len(workspace) -1]
		}
		_,path := filepath.Split(name)
		path = path + ".log"
		logDir := common.LogDIR
		logPath := filepath.Join(logDir,volumeName,path)
		os.MkdirAll(filepath.Join(logDir,volumeName),0755)
		file,err := os.OpenFile(logPath,os.O_CREATE|os.O_WRONLY|os.O_APPEND,0644)
		if err != nil {
			logrus.Fatal(err)
		}
		//To set panic
		syscall.Dup2(int(file.Fd()), 2)
		logrus.SetOutput(file)
		logrus.SetReportCaller(true)

		IP := dirAddr[:strings.Index(dirAddr,":")]
		hook, err := logrus_syslog.NewSyslogHook("udp",fmt.Sprintf("%s:514",IP),syslog.LOG_LOCAL3,"")
		if err != nil {
			logrus.Error("Unable to connect to local syslog daemon")
		} else {
			logrus.AddHook(hook)
		}


		os.MkdirAll(filepath.Join(workspace,volumeName),0755)
		lockFileName := filepath.Join(workspace,volumeName,"lock")
		lock, e := os.Create(lockFileName)
		if e != nil {
			log.Fatalf("Failed to create log: %s", e)
			os.Exit(-1)
		}
		defer os.Remove(lockFileName)
		defer lock.Close()

		e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e != nil {
			log.Fatalf("Failed to get lock: %s", e)
			os.Exit(-1)
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

		StartServer(volumeName, workspace, dirAddr)
		wait := make(chan bool)
		<-wait

		return nil
	}

	app.Run(os.Args)
}
