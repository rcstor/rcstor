package dir

import (
	"fmt"
	"rcstor/common"
	"strings"
	"sync"
	"time"
)

type Brick struct {
	UUID         uuid.UUID `json:"UUID"`
	IP           string    `json:"IP"`
	Port         uint16    `json:"Port"`
	Dir          string    `json:"Dir"`
	PID          int
	LastPingTime time.Time
}

func (brick *Brick) Addr() string {
	return fmt.Sprintf("%s:%d", brick.IP, brick.Port)
}

type PlacementGroup struct {
	PGId    uint32                        `json:"PgID"`
	Version int                           `json:"Version"`
	Bricks  []uuid.UUID                   `json:"BrickIDs"`
	Index   [common.REPLICATION]uuid.UUID `json:"Index"`
}

type ServerInfo struct {
	IP  string   `json:"IP"`
	Dir []string `json:"Dir"`

	//Optional
	BrickId []uuid.UUID `json:"BrickId"`
}

type HttpServer struct {
	IP           string
	Port         uint16
	PID          int
	ClientPort   uint16
	LastPingTime time.Time
}

func (server *HttpServer) ClientAddr() string {
	return fmt.Sprintf("%s:%d", server.IP, server.ClientPort)
}

func (server *HttpServer) IsRegistered() bool {
	return server.PID > 0
}

type Volume struct {
	Version    int    `json:"Version"`
	VolumeName string `json:"VolumeName"`

	PGs     []PlacementGroup `json:"PlacementGroups"`
	Servers []ServerInfo     `json:"ServerInfos"`

	Bricks      map[uuid.UUID]*Brick `json:"-"`
	HttpServers []HttpServer         `json:"-"`

	Parameter VolumeParameter `json:"Parameter"`

	started bool
	mu      sync.RWMutex
}

func (volume *Volume) Print() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("########%s########\n", volume.VolumeName))
	for i, server := range volume.Servers {
		b.WriteString(fmt.Sprintf("IP\t%s\n\n", server.IP))
		for i, brick := range server.BrickId {
			if _, ok := volume.Bricks[brick]; ok {
				b.WriteString(fmt.Sprintf("Started\t%+v\n", volume.Bricks[brick]))
			} else {
				b.WriteString(fmt.Sprintf("Stoped\t%s\n", server.Dir[i]))
			}
		}
		b.WriteString("\n")
		if i < len(volume.HttpServers) && volume.HttpServers[i].IsRegistered() {
			b.WriteString(fmt.Sprintf("HTTP Server started: %+v\n", volume.HttpServers[i]))
		} else {
			b.WriteString(fmt.Sprintf("HTTP Server stopped.\n"))
		}
		b.WriteString(fmt.Sprintf("--------------------------\n"))
	}
	return b.String()
}

type VolumeParameter struct {
	Layout   common.Layout `json:"Layout"`
	NumberPG int           `json:"nPG"`

	K          int `json:"K"`
	Redundancy int `json:"Redundancy"`
	//Only used for lrc code
	LocalRedundancy int `json:"LocalRedundancy"`

	//blocksize for contiguous, stipsize for stripe layout.
	BlockSize uint64 `json:"BlockSize"`

	MinBlockSize  uint64 `json:"MinBlockSize"`
	MaxBlockSize  uint64 `json:"MaxBlockSize"`
	GeometricBase int    `json:"GeometricBase"`

	Alignment uint64 `json:"Alignment"`

	IsSSD    bool `json:"IsSSD"`
	HTTPPort int  `json:"HTTPPort"`

	PlacementAlgorithm common.PlacementAlgorithm `json:"PlacementAlgorithm"`
}
