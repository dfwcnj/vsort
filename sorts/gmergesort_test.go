package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_gmergesort(t *testing.T) {

	//ls := []int64{1, 2, 1 << 4, 1 << 8, 1 << 16, 1 << 20}
	ls := []int64{1 << 23}
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true

	for _, nl := range ls {
		for _, r := range bools {
			log.Print("gmergesort strings test ", r)

			var l int = 32
			lns := randomdata.Randomstrings(nl, l, r)
			if len(lns) != int(nl) {
				log.Fatal("gmergesort test randomdata lns: wanted len ", nl, " got ", len(lns))
			}

			t0 := time.Now()
			slns := gmergesort(lns)
			log.Printf("gmergesort test duration %v", time.Since(t0))

			if len(slns) != int(nl) {
				log.Fatal("gmergesort test slns: wanted len ", nl, " got ", len(slns))
			}

			if !slices.IsSorted(slns) {
				t.Error("gmergesort failed for size ", nl)
			} else {
				log.Print("gmergesort test passed for ", nl)
			}
		}

	}

	for _, nl := range ls {
		log.Print("gmergesort uints test")

		lns := randomdata.Randomuints(nl)
		if len(lns) != int(nl) {
			log.Fatal("gmergesort test randomdata lns: wanted len ", nl, " got ", len(lns))
		}

		t0 := time.Now()
		slns := gmergesort(lns)
		log.Printf("gmergesort test duration %v", time.Since(t0))

		if len(slns) != int(nl) {
			t.Fatal("gmergesort test slns: wanted len ", nl, " got ", len(slns))
		}

		if !slices.IsSorted(slns) {
			t.Fatal("gmergesort failed for size ", nl)
		} else {
			log.Print("gmergesort test passed for ", nl)
		}

	}
}
