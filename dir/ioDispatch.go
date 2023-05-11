package dir

import (
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"rcstor/common"
)

type MoveInstruct struct {
	PG       PlacementGroup
	Broken   int
	NewBrick uuid.UUID
}

type RecoverReply []MoveInstruct

func (volume *Volume) findMin(io map[uuid.UUID]float64, ioServer []int, broken uuid.UUID) (uuid.UUID, float64) {

	var res uuid.UUID
	minS := math.MaxInt32
	minIO := math.MaxFloat64

	//brokenServer := volume.getBrickServerIndex(broken)

	for k, v := range io {
		server := volume.getBrickServerIndex(k)

		if ioServer[server] < minS {
			minS = ioServer[server]
			minIO = v
			res = k
		} else if ioServer[server] == minS && v < minIO {
			minIO = v
			res = k
		} else if ioServer[server] == minS && v == minIO && k.String() < res.String() {
			res = k
		}

	}
	return res, minIO
}

func printIO(io map[uuid.UUID]float64) {
	for k, v := range io {
		log.Infoln(k, "io:", v)
	}
}

func (volume *Volume) getBrickServerIndex(brickId uuid.UUID) int {
	for index, server := range volume.Servers {
		if server.IP == volume.Bricks[brickId].IP {
			return index
		}
	}
	return -1
}

func (volume *Volume) getIthBrick(i int) (int, uuid.UUID) {
	if i < 0 {
		panic("Invalid index")
	}
	index := 0
	for serverId, server := range volume.Servers {
		if i-index < len(server.BrickId) {
			return serverId, server.BrickId[i-index]
		}
		index += len(server.Dir)
	}
	panic("Brick not found")
}

var randGenerator *rand.Rand

func init() {
	randGenerator = rand.New(rand.NewSource(0))
}

func shuffle(bricks []uuid.UUID) {
	n := len(bricks)
	for i := 0; i+1 < n; i++ {
		j := int(randGenerator.Int31n(int32(n-i))) + i
		if j != i {
			bricks[i], bricks[j] = bricks[j], bricks[i]
		}
	}
}

// (n,k) rs code, when the i_th node failes, the cost required to read data from the jth node
func get_element_for_rs_repair_matrix(n, k, i, j int) int {
	ret := 1
	if i == j {
		ret = 0
	}
	return ret
}

// (k,l,n - k) lrc code, which divides data into l groups, computing 1 local parity and n-k global parity.
// The element represent when the i_th node failes, the cost required to read data from the jth node
func get_element_for_lrc_repair_matrix(n, k, l, i, j int) int {
	ret := 1

	nodes_per_group := k / l
	broken_group := i / nodes_per_group
	my_group := j / nodes_per_group

	if my_group == broken_group {
		ret = 1
	} else {
		ret = 0
	}

	if broken_group == l {
		if my_group == broken_group {
			ret = 0
		} else {
			ret = 1
		}
	}

	if i == j {
		ret = 0
	}

	return ret
}

// (n,k,n-1) clay code, when the i_th node failes, the cost required to read data from the jth node
func get_element_for_clay_repair_matrix(n, k, i, j int) int {
	ret := 1
	if i == j {
		ret = 0
	}
	return ret
}

func (volume *Volume) get_element_for_repair_matrix(i, j int) int {
	if volume.Parameter.Layout == common.RS {
		return get_element_for_rs_repair_matrix(volume.Parameter.K+volume.Parameter.Redundancy, volume.Parameter.K, i, j)
	} else if volume.Parameter.Layout == common.LRC {
		return get_element_for_lrc_repair_matrix(volume.Parameter.K+volume.Parameter.Redundancy, volume.Parameter.K, 3, i, j)
	} else {
		return get_element_for_clay_repair_matrix(volume.Parameter.K+volume.Parameter.Redundancy, volume.Parameter.K, i, j)
	}
}

func (volume *Volume) assignPGs() {
	if volume.Parameter.PlacementAlgorithm == common.Greedy {
		volume.greedyAssignPGs()
	} else if volume.Parameter.PlacementAlgorithm == common.Random {
		volume.randomAssignPGs()
	}
}

func (volume *Volume) randomAssignPGs() {
	volume.PGs = make([]PlacementGroup, volume.Parameter.NumberPG)

	bricks := len(volume.Bricks)

	for i := 0; i < volume.Parameter.NumberPG; i++ {

		pg := PlacementGroup{}
		pg.Bricks = make([]uuid.UUID, volume.Parameter.K+volume.Parameter.Redundancy)
		pg.Version = 0

		bUsed := make(map[int]bool)
		choosed := make([]int, 0)

		for j := 0; j < len(pg.Bricks); j++ {
			chosen := rand.Intn(bricks)
			for {
				ok, exist := bUsed[chosen]
				if !ok || !exist {
					break
				}
				chosen = rand.Intn(bricks)
			}
			bUsed[chosen] = true
			choosed = append(choosed, chosen)
		}

		for j := 0; j < common.REPLICATION; j++ {
			pg.Index[j] = pg.Bricks[j]
		}

		pg.PGId = uint32(i)

		volume.PGs[i] = pg
	}
}

const epsilon float64 = 0.02

// Recovery dispatch algorithms
func (volume *Volume) greedyAssignPGs() {
	volume.PGs = make([]PlacementGroup, volume.Parameter.NumberPG)

	servers := len(volume.Servers)
	bricks := len(volume.Bricks)

	wv := make([]int, bricks)
	graph := make([][]int, bricks)
	num_pgs := make([]int, bricks)

	for i := 0; i < bricks; i++ {
		graph[i] = make([]int, bricks)
		for j := 0; j < bricks; j++ {
			graph[i][j] = 0
		}
	}

	for i := 0; i < volume.Parameter.NumberPG; i++ {

		pg := PlacementGroup{}
		pg.Bricks = make([]uuid.UUID, volume.Parameter.K+volume.Parameter.Redundancy)
		pg.Version = 0

		serUsed := make([]int, servers)
		bUsed := make(map[int]bool)

		for j := 0; j < servers; j++ {
			serUsed[j] = 0
		}

		choosed := make([]int, 0)

		//Line 3 of Algorithm 1
		for j := 0; j < len(pg.Bricks); j++ {
			minSerUsed := common.MinArray(serUsed)
			minPGs := math.MaxInt16

			minLoad := math.MaxInt32
			minWV := math.MaxInt32

			for t := 0; t < bricks; t++ {
				if num_pgs[t] <= minPGs {
					minPGs = num_pgs[t]
				}
			}

			var chosen int

			for t := 0; t < bricks; t++ {
				s, _ := volume.getIthBrick(t)
				used, exist := bUsed[t]

				//Line 4 of Algorithm 1
				if exist && used {
					continue
				}

				//Line 5 of Algorithm 1
				if serUsed[s] != minSerUsed {
					continue
				}

				//Line 6 of Algorithm 1
				if num_pgs[t] > int(float64(minPGs)*(1+epsilon)) {
					continue
				}

				load := 0

				//When executing to obtain the first element of new_pg, the result is Line 1 of Algorithm.
				//Otherwise the result is Line 7 of the algorithm
				for m := 0; m < len(choosed); m++ {
					load += graph[t][choosed[m]] + volume.get_element_for_repair_matrix(len(choosed), m)
				}

				if load < minLoad || (load == minLoad && wv[t] < minWV) {
					chosen = t
					minLoad = load
					minWV = wv[t]
				}

			}
			s, _ := volume.getIthBrick(chosen)
			serUsed[s]++
			bUsed[chosen] = true

			choosed = append(choosed, chosen)
		}

		for j := 0; j < len(choosed); j++ {
			for k := 0; k < len(choosed); k++ {
				if j != k {
					graph[choosed[j]][choosed[k]]++
					wv[choosed[j]] += volume.get_element_for_repair_matrix(k, j)
					wv[choosed[k]] += volume.get_element_for_repair_matrix(k, j)
				}
			}
		}

		for j := 0; j < len(choosed); j++ {
			_, brickId := volume.getIthBrick(choosed[j])
			pg.Bricks[j] = brickId
			num_pgs[choosed[j]]++
		}

		shuffle(pg.Bricks)

		for j := 0; j < common.REPLICATION; j++ {
			pg.Index[j] = pg.Bricks[j]
		}

		pg.PGId = uint32(i)

		volume.PGs[i] = pg
	}
}

type RecoveryDispatchArgs struct {
	BrokenBrick uuid.UUID
	VolumeName  string
}

func (volume *Volume) dispatch(args *RecoveryDispatchArgs, reply *RecoverReply) error {
	io := make(map[uuid.UUID]float64)
	res := make(RecoverReply, 0)

	for brickID, _ := range volume.Bricks {
		if brickID != args.BrokenBrick {
			io[brickID] = 0
		}
	}

	for i := 0; i < len(volume.PGs); i++ {
		pg := volume.PGs[i].Bricks

		broken := -1
		for j := 0; j < len(pg); j++ {
			if pg[j] == args.BrokenBrick {
				broken = j
				break
			}
		}
		cost := 1.0
		if broken < 4 && broken >= 0 {
			cost = 1.0
		} else if broken >= 4 && broken < 8 {
			cost = 1.1
		} else if broken >= 8 && broken < 12 {
			cost = 1.4
		} else {
			cost = 2.0
		}
		if broken >= 0 {
			for j := 0; j < len(pg); j++ {
				if pg[j] != args.BrokenBrick {
					currentIo, exist := io[pg[j]]
					if !exist {
						io[pg[j]] = cost
					} else {
						io[pg[j]] = currentIo + cost
					}
				}
			}
		} else {
			continue
		}
	}

	ioServer := make([]int, len(volume.Servers))
	for i, _ := range volume.Servers {
		ioServer[i] = 0
	}

	for i := 0; i < len(volume.PGs); i++ {
		pg := volume.PGs[i].Bricks
		broken := -1
		for j := 0; j < len(pg); j++ {
			if pg[j] == args.BrokenBrick {
				broken = j
				break
			}
		}
		if broken >= 0 {
			minBrick, cnt := volume.findMin(io, ioServer, args.BrokenBrick)
			io[minBrick] = cnt + 4.0 //??? RS:1, MSR:2 or 3 or 4, LRC:1
			ioServer[volume.getBrickServerIndex(minBrick)]++
			res = append(res, MoveInstruct{PG: volume.PGs[i], Broken: broken, NewBrick: minBrick})
		}
	}

	//printIO(io)

	*reply = res
	return nil
}

func (service *DirectoryService) RecoverDispatch(args *RecoveryDispatchArgs, reply *RecoverReply) error {

	loaded, ok := service.Volumes.Load(args.VolumeName)
	if !ok {
		return common.ErrVolumeNotExist
	}
	volume := loaded.(*Volume)
	volume.mu.RLock()
	defer volume.mu.RUnlock()

	return volume.dispatch(args, reply)
}
