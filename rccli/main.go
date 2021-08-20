package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"io/ioutil"
	"math/rand"
	"os"
	"rcstor/common"
	"rcstor/dir"
	"rcstor/rcclient"
	"strconv"
	"sync"
	"time"
)

func generateParity(client *rcclient.Client, pg_start, pg_end int) {

	var wg sync.WaitGroup

	clients := 4
	ch := make(chan int)
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func() {
			for pgID := range ch {
				log.Println("Begin to generate parity for", pgID)
				client.GeneratePGParity(pgID)
				log.Println("Finish generate parity for", pgID)
			}
			wg.Done()
		}()
	}
	for i := pg_start; i < pg_end; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
}


func main() {

	rand.Seed(time.Now().UnixNano())

	app := cli.NewApp()
	app.Name = "RCClient"
	app.Description = "Client for rcstor"

	var dirAddr string

	app.Version = "1.0"
	app.EnableBashCompletion = true

	log.SetReportCaller(true)

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "dirAddr",
			Value:       fmt.Sprintf("localhost:%s",strconv.Itoa(common.DefaultDirPort)),
			Usage:       "the address for directory service",
			Destination: &dirAddr,
		},
	}

	app.Commands = []cli.Command{
		{
			Name: "list",
			Usage : "List volumes",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				if c.NArg() < 1 {
					var args int
					var reply []dir.Volume
					err := dirConn.Call("DirectoryService.GetAllVolumes", &args, &reply)
					if err != nil {
						log.Errorln(err)
						return err
					}

					for _, volume := range reply {
						log.Println(volume.Print())
					}
				} else {
					volumeName := c.Args().First()
					var volume dir.Volume
					err := dirConn.Call("DirectoryService.GetVolume",&volumeName,&volume)
					if err != nil {
						log.Errorln(err)
						return err
					}
					log.Println(volume.Print())
				}
				return nil
			},
		},
		{
			Name: "create",
			Usage : "Create a volume from json file",
			Action: func(c *cli.Context) error {
				file,err := os.Open(c.Args().First())
				if err != nil {
					log.Errorln(err)
					return err
				}
				content,err := ioutil.ReadAll(file)
				if err != nil {
					log.Errorln(err)
					return err
				}

				var args dir.CreateVolumeArgs
				err = json.Unmarshal(content,&args)

				log.Println(args)

				if err != nil {
					log.Errorln(err)
					return err
				}

				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()

				var reply interface{}

				err = dirConn.Call("DirectoryService.CreateVolume",&args,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}
				return nil
			},
		},

		{
			Name: "start",
			Usage : "Start a volume",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply interface{}
				err := dirConn.Call("DirectoryService.StartVolume",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},

		{
			Name: "stop",
			Usage : "Stop a volume",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply interface{}

				err := dirConn.Call("DirectoryService.StopVolume",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},

		{
			Name: "drop",
			Usage : "Drop a volume",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply interface{}
				err := dirConn.Call("DirectoryService.DropVolume",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},

		{
			Name: "tracePuts",
			Usage : "Put objects using traces",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply interface{}
				err := dirConn.Call("DirectoryService.TracePuts",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},

		{
			Name: "genParity",
			Usage : "Generate Parity",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply interface{}
				err := dirConn.Call("DirectoryService.GenerateParity",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},

		{
			Name: "traceDgets",
			Usage : "Evaluate dgets using traces",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()
				var reply interface{}

				err := dirConn.Call("DirectoryService.TraceDget",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},
		{
			Name: "foregroundTraceGets",
			Usage : "Add foreground get requests according to the trace",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply interface{}
				err := dirConn.Call("DirectoryService.ForegroundTraceGets",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},
		{
			Name: "recovery",
			Usage : "Recovery the first brick",
			Action: func(c *cli.Context) error {
				dirConn := common.MakeConnection(dirAddr)
				defer dirConn.Close()
				name := c.Args().First()

				var reply dir.Volume

				err := dirConn.Call("DirectoryService.GetVolume",&name,&reply)
				if err != nil {
					log.Errorln(err)
					return err
				}
				log.Infoln(reply.Servers[0].BrickId[0])

				var args dir.RecoveryDispatchArgs
				args.VolumeName = name
				args.BrokenBrick = reply.Servers[0].BrickId[0]
				var rec interface{}

				err = dirConn.Call("DirectoryService.Recovery",&args,&rec)
				if err != nil {
					log.Errorln(err)
					return err
				}

				return nil
			},
		},


	}

	app.Run(os.Args)
}
