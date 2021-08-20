package common

import (
	"math"
	"math/rand"
	"time"
)

func Min(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func IsStriped(layout Layout) bool {
	if layout == Contiguous || layout == Geometric || layout == Hitchhiker {
		return false
	} else {
		return true
	}
}

var randGenerator *rand.Rand

func init() {
	randGenerator = rand.New(rand.NewSource(100))
}

type TimeStats struct {
	repairTime []time.Duration
	transferTime []time.Duration
}

func (stats *TimeStats) AddTime(repair,transfer time.Duration) {
	if stats.repairTime == nil{
		stats.repairTime = make([]time.Duration,0)
	}
	if stats.transferTime == nil{
		stats.transferTime = make([]time.Duration,0)
	}
	stats.repairTime = append(stats.repairTime,repair)
	stats.transferTime = append(stats.transferTime,transfer)
}

func (stats *TimeStats) Average() (repair,transfer int64) {
	for _,rep := range stats.repairTime {
		repair += rep.Nanoseconds()
	}
	for _,trans := range stats.transferTime {
		transfer += trans.Nanoseconds()
	}
	repair /= int64(len(stats.repairTime))
	transfer /= int64(len(stats.transferTime))
	return repair,transfer
}

func OrderShuffle(n int) []int {
	list := make([]int, n)
	for i := 0; i < n; i++ {
		list[i] = i
	}
	for i := 0; i+1 < n; i++ {
		j := int(rand.Int31n(int32(n-i))) + i
		if j != i {
			tmp := list[i]
			list[i] = list[j]
			list[j] = tmp
		}
	}
	return list
}



//
//func Shuffle(volumes []dir.Brick) []dir.Brick {
//	n := len(volumes)
//	for i := 0; i+1 < n; i++ {
//		j := int(rand.Int31n(int32(n-i))) + i
//		if j != i {
//			volumes[i], volumes[j] = volumes[j], volumes[i]
//		}
//	}
//	return volumes
//}

func RandNormalDistribution(mu float64, sd float64) float64 {
	u1, u2 := rand.Float64(), rand.Float64()
	theta := 2 * math.Pi * u1
	R := math.Sqrt(-2 * math.Log(u2))
	return R*math.Cos(theta)*sd + mu
}

func NormalDistribution(mu float64, sd float64, num int) []float64 {
	ret := make([]float64, num)
	for i := 0; i < num; i++ {
		ret[i] = RandNormalDistribution(sd, mu)
	}
	return ret
}

func MinArray(a []int) int {
	if len(a) == 0 {
		return math.MinInt32
	}
	ret := a[0]
	for _, x := range a {
		if x < ret {
			ret = x
		}
	}
	return ret
}
func MinFloatArray(a []float64) (float64,int) {
	if len(a) == 0 {
		return math.MinInt32,0
	}
	ret := a[0]
	index := 0
	for i, x := range a {
		if x < ret {
			ret = x
			index = i
		}
	}
	return ret,index
}

func Pow(a, b int) int {
	if b == 0 {
		return 1
	}
	if b == 1 {
		return a
	}
	if b%2 == 0 {
		mid := Pow(a, b/2)
		return mid * mid
	} else {
		mid := Pow(a, b/2)
		return mid * mid * a
	}
}
