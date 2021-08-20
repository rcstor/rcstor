package common

import (
	"sync"
)

type ConnectionPool struct {
	connections sync.Map
}

func (pool *ConnectionPool) GetConnection(addr string) *Connection {
	conn,ok := pool.connections.Load(addr)
	if !ok{
		connection := MakeConnection(addr)
		pool.connections.Store(addr,connection)
		return connection
	} else {
		return conn.(*Connection)
	}
}

func (pool *ConnectionPool) ResetAddress(origin,dest string) {
	conn,ok := pool.connections.Load(origin)
	if !ok{
		return
	}
	conn.(*Connection).ResetAddress(dest)
}

func MakeConnectionPool() *ConnectionPool {
	return &ConnectionPool{connections: sync.Map{}}
}

func (pool *ConnectionPool) Release() {
	pool.connections.Range(func(k, v interface{}) bool {
		conn := v.(*Connection)
		if conn != nil {
			conn.Close()
		}
		return true
	})

}