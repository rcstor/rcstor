# RCStor
`RCStor` is a distributed object storage system designed to utilize regenerating codes.

## Installation
We need to switch to the root user. On Ubuntu, run ```sudo su```.

Install golang-1.14:
```sh
wget https://dl.google.com/go/go1.14.10.linux-amd64.tar.gz
tar -xvf go1.14.10.linux-amd64.tar.gz -C /usr/local/
```
Set environmental variable:
```sh
echo 'export GOROOT=/usr/local/go' >> ~/.profile
echo 'export GOPATH=$HOME/go' >> ~/.profile
echo 'export PATH=$GOPATH/bin:$GOROOT/bin:/usr/local/bin:$PATH' >> ~/.profile
source ~/.profile
```
Install `rcstor`:
```sh
git clone https://github.com/rcstor/rcstor.git
cd rcstor
make install
```

We only need to install `rcstor` on one server. All binaries will be distributed to other machines through ssh automatically.

## Configure SSH
We need to start SSH services on every machine and copy the key of each server to every other servers.
```sh
apt update
apt install openssh-client openssh-server
systemctl start sshd
ssh-keygen
ssh-copy-id root@<IP of all machines>
```
We need to run the above commands on all servers.
## Start Directory Service

Run the following command to start directory service.
```sh
rcdir start
```

## Configure Volume File
We need to create a json file to specify all the parameters needed to create a volume:
- The IPs of all servers
- The paths of all disks
- Layout (e.g. Geometric, Stripe)
- Layout parameter
- Disk type (i.e. HDD or SSD)
- The exposed port of HTTP server.

Refer to `Geo-1M.json` and `Con-1M.json` as the example for volume json file.

## Create And Start The Volume

Run the following command on directory server to create and start the volume.
```sh
rccli create <JsonFileName>
rccli start <VolumeName>
```

Run `rccli list` to check the status of the volume.

## Put/Get an object
We can put/get an object from any server. We need to specify an unused object id to put the object into `rcstor`.

PUT: ```wget --post-file=<filename> http://<ServerIP>:<HTTPPort>/put/<objectID>```

GET: ```wget http://<ServerIP>:<HTTPPort>/get/<objectID>```

## Test Degraded Read Time of an Object
We need to run ```rccli genParity <VolumeName>``` to generate the parity before testing degraded read.
We can find the running result in `/usr/local/var/log/rcdir.log`. We can proceed only after we find that the operation has completed from the log.
Then, we can run the following command to test the degraded time to read an object, `rcstor` will assume one disk is broken and repair the object.

DGET: ```wget http://<ServerIP>:<HTTPPort>/dget/<objectID>```

## Import Data According to the trace
We need to make sure the volume is clean (no object has been put yet) before importing.
Run ```rccli tracePuts <VolumeName>``` to generate random data sampled from the trace.
This may take very long time and involve writing a very large amount of data. We need to check the log to make sure this operation has been completed before moving foward.
Then, we need to run ```rccli genParity <VolumeName>``` to generate parity for the imported data.

## Test Recovery Time
Run ```rccli recovery <VolumeName>```, `rcstor` will think one of the disk is broken and begin to recover. Then we can start to see the recovery procedure in the log.

## Test Average Degraded Read Time
Run ```rccli traceDgets <VolumeName>```, then we can see the result of degraded read in the log.

## Test Degreaded Read Time Under Foreground Requests
Run ```rccli foregroundTraceGets <VolumeName>``` to start foreground requests, then we run ```rccli traceDgets <VolumeName>``` to test degraded read time. We can stop the volume by running ```rccli stop <VolumeName>``` afterwards.

## Drop the Volume
Run ```rccli drop <VolumeName>```, then we can delete the data for that volume.

## Performance Tuning
We can use huge pages to improve the performance of `rcstor`.
Run ```echo 1000 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages```