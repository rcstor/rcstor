package dir

import (
	"errors"
	"fmt"
	"github.com/bramvdbogaerde/go-scp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"rcstor/common"
	"strconv"
	"sync"
	"time"
)

var sshConfig *ssh.ClientConfig
var clients sync.Map

var NUMA_CTL = "numactl --cpunodebind=1 --membind=1 "
//const NUMA_CTL = " "

func setNuma() {
	_,err := exec.LookPath("numactl")
	if err != nil{
		//log.Warnln("numactl not found, disabled.")
		NUMA_CTL=""
	}
}

func init() {
	setNuma()
	usr, _ := user.Current()
	dir := usr.HomeDir
	usrName := usr.Username

	key, err := ioutil.ReadFile(filepath.Join(dir,".ssh","id_rsa"))
	if err != nil {
		log.Fatalf("unable to read private key: %v", err)
	}

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Fatalf("unable to parse private key: %v", err)
	}

	sshConfig = &ssh.ClientConfig{
		User: usrName,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
}

func copyFile(ip,path string) {
	client := scp.NewClient(fmt.Sprintf("%s:22",ip), sshConfig)

	// Connect to the remote server
	err := client.Connect()
	if err != nil {
		log.Errorln("Couldn't establish a connection to the remote server ", err)
		return
	}

	// Open a file
	f, _ := os.Open(path)

	// Close client connection after the file has been copied
	defer client.Close()

	// Close the file after it has been copied
	defer f.Close()

	// Finally, copy the file over
	// Usage: CopyFile(fileReader, remotePath, permission)

	err = client.CopyFile(f, path, "0755")

	if err != nil {
		fmt.Println("Error while copying file ", err)
	}
}

func getSession(ip string) (session *ssh.Session) {
	var err error

	conn,ok := clients.Load(ip)

	if !ok {
		conn, err = ssh.Dial("tcp", ip+":22", sshConfig)
		if err != nil {
			log.Errorln(err)
		}

		clients.Store(ip,conn)
	}
	session,err = conn.(*ssh.Client).NewSession()
	for err != nil{
		conn, err = ssh.Dial("tcp", ip+":22", sshConfig)
		if err != nil || conn == nil{
			log.Errorln(err)
			time.Sleep(time.Second)
		}
		clients.Store(ip,conn)
		session,err = conn.(*ssh.Client).NewSession()
	}
	return session
}

//We need a connection for each command,otherwise errors may be returned.
//Fixme: memory leak here.
func execRemoteAsync(command string, ip string) error {
	session := getSession(ip)

	err := session.Start(command)
	//defer session.Close()

	if err != nil {
		return err
	}

	return nil
}

func execRemoteSync(command string, ip string) error {

	session := getSession(ip)
	defer session.Close()

	err := session.Run(command)

	if err != nil {
		return err
	}

	return nil
}

func killAllBricks(ip string) error {
	//return nil
	err := execRemoteSync("pkill "+common.StorFilePath, ip)
	var exit *ssh.ExitError
	if errors.As(err,&exit) {
		return nil
	}
	return err
}

//Fixme: should not killed.
func killAllHttp(ip string) error {
	err := execRemoteSync("pkill "+common.HTTPFilePath, ip)
	var exit *ssh.ExitError
	if errors.As(err,&exit) {
		return nil
	}
	return err
}

func killAllClient(ip string) error {
	err := execRemoteSync("pkill rccli", ip)
	var exit *ssh.ExitError
	if errors.As(err,&exit) {
		return nil
	}
	return err
}

func clearCache(ip string) error {
	return execRemoteSync("echo 3 > /proc/sys/vm/drop_caches", ip)
}

func startHTTP(volumeName string, ip string, myAddr string) {
	err := killAllHttp(ip)
	if err != nil{
		log.Errorln(err)
	}
	err = clearCache(ip)
	if err != nil{
		log.Errorln("Failed to clear cache",err)
	}

	copyFile(ip,common.HTTPFilePath)

	command := NUMA_CTL + common.HTTPFilePath + " --volume=" + volumeName + " --dirAddr=" + myAddr
	log.Println(ip, command)
	if err := execRemoteAsync(command, ip); err != nil {
		log.Errorln(ip, err)
	}
	log.Println("Deploy http successfully at", ip)
}

func startServerBricks(server ServerInfo, volumeName, myAddr string) {
	err := killAllBricks(server.IP)
	if err != nil {
		log.Errorln(err)
	}
	copyFile(server.IP,common.StorFilePath)

	var wg sync.WaitGroup
	wg.Add(len(server.Dir))
	for _, brick := range server.Dir {
		go func(dir string) {
			command := NUMA_CTL + common.StorFilePath + " --workspace=" + dir + " --dirAddr=" + myAddr + " --volume=" + volumeName
			log.Println(server.IP, command)
			err := execRemoteAsync(command, server.IP)

			if err != nil {
				log.Errorln(server.IP, err)
			}
			wg.Done()
		}(brick)
	}
	wg.Wait()
	log.Println("Deploy rcstor successfully at", server.IP)
}

func startBrick(dir, IP, volumeName, myAddr string) {

	command := NUMA_CTL + common.StorFilePath + " --workspace=" + dir + " --dirAddr=" + myAddr + " --volume=" + volumeName
	log.Println(IP, command)
	err := execRemoteAsync(command, IP)

	if err != nil {
		log.Errorln(IP, err)
	}

}

func restartBrick(brick *Brick, volumeName, myAddr string) {
	log.Infoln("restaring", brick)
	err := execRemoteSync("kill -9 "+strconv.Itoa(brick.PID), brick.IP)
	var exit *ssh.ExitError
	if err != nil && !errors.As(err,&exit){
		log.Errorln("killing brick on", brick.IP, err)
	}
	startBrick(brick.Dir,brick.IP,volumeName,myAddr)
}

func restartHttp(server *HttpServer, volumeName, myAddr string) {
	log.Infoln("restaring", server)

	var exit *ssh.ExitError
	err := execRemoteSync("kill -9 "+strconv.Itoa(server.PID), server.IP)
	if err != nil && !errors.As(err,&exit){
		log.Errorln("killing http server on", server.IP, err)
	}

	startHTTP(volumeName,server.IP,myAddr)
}

func (volume *Volume) stopStorageServer(server ServerInfo) {

	var wg sync.WaitGroup

	for _, brick := range server.BrickId {
		if _, ok := volume.Bricks[brick]; !ok {
			continue
		}
		wg.Add(1)
		go func(brick *Brick) {
			command := "kill -9 " + strconv.Itoa(brick.PID)
			log.Infoln(fmt.Sprintf("%s:%s", server.IP, brick.Dir), command)
			err := execRemoteSync(command, server.IP)

			if err != nil {
				log.Errorln(server.IP, err)
			}
			wg.Done()
		}(volume.Bricks[brick])
	}
	wg.Wait()
	log.Printf("Stop %s brick successfully at %s\n", volume.VolumeName, server.IP)
}

func (volume *Volume) stopHttpServer(ip string, pid int) {

	command := "kill -9 " + strconv.Itoa(pid)
	log.Infoln(fmt.Sprintf("%s", ip), command)
	err := execRemoteSync(command, ip)
	if err != nil {
		log.Errorln(ip, err)
	}

	log.Printf("Stop %s http successfully at %s\n", volume.VolumeName, ip)
}

func copyClient(ip string) {
	killAllClient(ip)
	cmd := exec.Command("/bin/sh", "-c", "scp /root/bin/rccli "+ip+":/usr/local/rccli")
	err := cmd.Run()
	if err != nil {
		log.Fatalln(ip, err)
	}
}
