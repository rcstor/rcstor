package main

import (
	"github.com/sirupsen/logrus"
	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
	"github.com/takama/daemon"
	"github.com/urfave/cli"
	"io"
	"log/syslog"
	"os"
	"os/signal"
	"path/filepath"
	"rcstor/common"
	"rcstor/dir"
	"runtime"
	"syscall"
)

var port uint
var logPath string
var configPath string

func main() {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("Panic: %v", r)
			os.Exit(1)
		}
	}()

	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.Name = "RCDirectory"
	app.Description = "Directory Service for rcstor"

	app.Version = "0.1"
	app.EnableBashCompletion = true

	app.Flags = []cli.Flag{
		cli.UintFlag{
			Name:        "port",
			Value:       common.DefaultDirPort,
			Usage:       "Port of directory service",
			Destination: &port,
		},
		cli.StringFlag{
			Name:        "configPath",
			Value:       "/usr/local/var/rcdir/",
			Usage:       "path to store directory config",
			Destination: &configPath,
		},
		cli.StringFlag{
			Name:        "logPath",
			Value:       filepath.Join(common.LogDIR, "dir.log"),
			Usage:       "path to store directory log",
			Destination: &logPath,
		},
	}

	var kind daemon.Kind
	if runtime.GOOS == "darwin" {
		kind = daemon.UserAgent
	} else {
		kind = daemon.SystemDaemon
	}
	service, err := daemon.New("rcdir", "Directory Service for RCStor", kind)
	if err != nil {
		logrus.Fatalln(err)
	}

	app.Action = func(c *cli.Context) error {
		logFile, _ := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE | os.O_APPEND, 0666)
		defer logFile.Close()

		logrus.SetReportCaller(true)

		logrus.SetOutput(io.MultiWriter(logFile, os.Stdout))

		hook, err := logrus_syslog.NewSyslogHook("udp", "localhost:514", syslog.LOG_LOCAL1, "")
		if err != nil {
			logrus.Error("Unable to connect to local syslog daemon")
		} else {
			logrus.AddHook(hook)
		}

		if !common.PortAvailable(uint16(port)) {
			logrus.Fatalln("Port in use")
			os.Exit(-1)
		}

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

		dir.StartDirectoryService(configPath, uint16(port))

		ch := make(chan bool, 0)
		select {
		case <-interrupt:
			os.Exit(-1)
		case <-ch:
			return nil
		}
		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:  "start",
			Usage: "Start Directory Service",
			Action: func(c *cli.Context) error {

				status, err := service.Status()

				if err == daemon.ErrNotInstalled {
					status, err = service.Install()
					if err != nil {
						logrus.Fatal(status, err)
						return err
					}
				}

				status, err = service.Start()
				if err != nil {
					logrus.Fatal(status, err)
					return err
				}

				return nil
			},
		},
		{
			Name:  "stop",
			Usage: "Stop Directory Service",
			Action: func(c *cli.Context) error {

				status, err := service.Stop()
				if err != nil {
					logrus.Fatal(status, err)
					return err
				}

				return nil
			},
		},
		{
			Name:  "restart",
			Usage: "Restart Directory Service",
			Action: func(c *cli.Context) error {

				status, err := service.Stop()
				if err != nil {
					logrus.Fatal(status, err)
					return err
				}

				status, err = service.Start()
				if err != nil {
					logrus.Fatal(status, err)
					return err
				}

				return nil
			},
		},
		{
			Name:  "status",
			Usage: "Check status of rcdir",
			Action: func(c *cli.Context) error {

				status, err := service.Status()
				logrus.Infoln(status)

				return err
			},
		},
	}

	app.Run(os.Args)
}
