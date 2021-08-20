package index

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"testing"
)

const iterNum = CheckPointInterval*3 + 3

func testIndexService(constructor IndexConstructor) {
	replys := make([]*ObjectIndex,iterNum)
	pgs := make([]uint32,iterNum)
	for i:=0;i<iterNum;i++ {
		pgs[i] = rand.Uint32()%10
	}

	service := MakeIndexService("./indexTest/",constructor)

	for i := 0; i < iterNum; i++ {
		args := &PutIndexArgs{
			ObjectID: uint64(i),
			Size:     uint64(rand.Int31n((256 << 20) - 1) + 1) ,
			PGId: pgs[i],
		}
		reply := ObjectIndex{}
		err := service.PutIndex(args, &reply)
		if err != nil {
			log.Errorln(err)
		}
		replys[i] = &reply
	}

	service = MakeIndexService("./indexTest/",constructor)
	for i := 0; i < iterNum; i++ {
		args := &GetIndexArgs{
			ObjectId: uint64(i),
			PGId:       pgs[i],
		}
		reply := ObjectIndex{}
		service.GetIndex(args, &reply)
		src := reply
		target := replys[i]

		if src.Contiguous != nil && *src.Contiguous != *target.Contiguous {
			log.Fatalln(*src.Contiguous,*target.Contiguous)
		}
		if src.StripeMax != nil && *src.StripeMax != *target.StripeMax {
			log.Fatalln(*src.StripeMax,*target.StripeMax)
		}
		if src.Geometric != nil {
			for i,block := range (*src.Geometric).Blocks {
				if block != (*target.Geometric).Blocks[i] {
					log.Fatalln(block,(*target.Geometric).Blocks[i])
				}
			}
		}
		if src.Replicated != nil && *src.Replicated != *target.Replicated {
			log.Fatalln(*src.Replicated,*target.Replicated)
		}
		if src.Stripe != nil && *src.Stripe != *target.Stripe {
			log.Fatalln(*src.Stripe,*target.Stripe)
		}

	}

	var args int
	var reply interface{}
	service.Drop(&args,&reply)

}

func TestContiguousIndexService(t *testing.T) {
	log.Println("Test: ContiguousIndexService ... ")

	testIndexService(&ContiguousIndexConstructor{K:10})

	log.Println("... Passed")
}

func TestGeometricIndexService(t *testing.T) {
	log.Println("Test: GeometricIndexService ... ")

	partitioner := GeometricPartitioner{MinBlock: uint64(4<<20),MaxBlock: uint64(256<<20),Base: 2}
	testIndexService(&GeometricIndexConstructor{K:10, Partitioner: partitioner})

	log.Println("... Passed")
}

func TestStripedIndexService(t *testing.T) {
	log.Println("Test: StripedIndexService ... ")

	testIndexService(&StripedIndexConstructor{K:10,StripSize: 256 << 10})

	log.Println("... Passed")
}

func TestStripedMaxIndexService(t *testing.T) {
	log.Println("Test: GeometricIndexService ... ")

	testIndexService(&StripedMaxIndexConstructor{K:10,Alignment: 512*256})
	log.Println("... Passed")
}
