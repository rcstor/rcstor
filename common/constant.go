package common

import "time"

const LogDIR = "/usr/local/var/log"
const HTTPFilePath = "/usr/local/bin/rchttp"
const StorFilePath = "/usr/local/bin/rcstor"

const DefaultDirPort = 30100
const HeartBeatInterval = time.Second * 5

const SSDMaxSize uint64 = 4 << 20
const HDDMaxSize uint64 = 4 << 30
const HDDNumObjects uint64 = 170000
const SSDNumObjects uint64 = 500000

const DefaultHTTPPort = 30888
const ForegroundClients = 8

const StorageOffsetAlign = 512

const REPLICATION = 3
const DefaultMaxObjectId = 10000

const RecoveryConcurrentNum = 16
const MaxTCPPerConn = 16

// We have a default rate limiter for 1Gbps
const MaxClientBandwidth uint64 = (1 << 27)

const rpcReconnectTimeout = time.Second

const RegisterTimeout = time.Second * 20
const pingInterval = time.Second * 10
const pingTimeout = time.Second * 1000

// Timeout to send 256KB buffer
const IOTimeout = time.Millisecond * 500

const StorReadMinSize uint64 = 256 << 10
const StorWriteMinSize uint64 = 256 << 10

type Layout string

const (
	Contiguous Layout = "Contiguous"
	Geometric         = "Geometric"
	Stripe            = "Stripe"
	StripeMax         = "StripeMax"
	RS                = "RS"
	LRC               = "LRC"
	Hitchhiker        = "Hitchhiker"
)

type Status int

const (
	Started Status = iota
	Stopped
	Degraded
	Recovering
)

type PlacementAlgorithm string

const (
	Greedy = "Greedy"
	Random = "Random"
)
