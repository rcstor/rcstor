package dir

import (
	log "github.com/sirupsen/logrus"
	"rcstor/common"
	"time"
	"sync"
	"github.com/satori/go.uuid"
)

type VolumeConnectionPool struct{
	*common.ConnectionPool
	volumeName string

	//A copy of volume
	volume     *Volume

	mu         sync.RWMutex
	dirConn    *common.Connection
	released   bool
}

func MakeVolumeConnectionPool(volumeName string, dirConn *common.Connection) *VolumeConnectionPool{
	pool := &VolumeConnectionPool{ConnectionPool: common.MakeConnectionPool(),volumeName: volumeName,dirConn: dirConn}
	pool.updateVolume()
	pool.released = false
	pool.autoUpdate()
	return pool
}
func (pool *VolumeConnectionPool) GetVolume() *Volume{
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.volume
}
func (pool *VolumeConnectionPool) GetConnectionByBrick(brickId uuid.UUID) *common.Connection{
	pool.mu.RLock()
	brick,ok := pool.volume.Bricks[brickId]
	for !ok || brick == nil {
		pool.mu.RUnlock()
		time.Sleep(time.Second)
		pool.updateVolume()
		pool.mu.RLock()
		brick,ok = pool.volume.Bricks[brickId]
	}
	pool.mu.RUnlock()
	return pool.GetConnection(brick.Addr())
}

func (pool *VolumeConnectionPool) GetClientConnectionById(id int) *common.Connection{
	pool.mu.RLock()
	server := pool.volume.HttpServers[id]
	for !server.IsRegistered() {
		pool.mu.RUnlock()
		time.Sleep(time.Second)
		pool.updateVolume()
		pool.mu.RLock()
		server = pool.volume.HttpServers[id]
	}
	pool.mu.RUnlock()
	return pool.GetConnection(server.ClientAddr())
}

func (pool *VolumeConnectionPool) GetDirConnection() *common.Connection{
	return pool.dirConn
}

func (pool *VolumeConnectionPool) updateVolume() {
	var volume Volume
	err := pool.dirConn.Call("DirectoryService.GetVolume",&pool.volumeName,&volume)
	if err != nil {
		log.Errorln("Error getting volume when auto updating:",err)
	}
	pool.mu.Lock()

	if pool.volume != nil {
		for id, client := range volume.HttpServers {
			if id < len(pool.volume.HttpServers) && pool.volume.HttpServers[id].IsRegistered() && pool.volume.HttpServers[id].ClientAddr() != client.ClientAddr() {
				pool.ResetAddress(pool.volume.HttpServers[id].ClientAddr(), client.ClientAddr())
			}
		}

		for id, brick := range volume.Bricks {
			oldBrick, ok := pool.volume.Bricks[id]
			if ok && oldBrick.Addr() != brick.Addr() {
				pool.ResetAddress(oldBrick.Addr(), brick.Addr())
			}
		}
	}

	pool.volume = &volume
	pool.mu.Unlock()
}

func (pool *VolumeConnectionPool) Release() {
	pool.mu.Lock()
	pool.released = true
	pool.ConnectionPool.Release()
	pool.mu.Unlock()
}

func (pool *VolumeConnectionPool) autoUpdate() {
	go func() {
		for {
			time.Sleep(common.HeartBeatInterval)
			pool.mu.Lock()
			released := pool.released
			pool.mu.Unlock()
			if released{
				return
			}
			pool.updateVolume()
		}
	}()
}

