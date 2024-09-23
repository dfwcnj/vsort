package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_gmergesort(t *testing.T) {

	//ls := []int64{1, 2, 1 << 4, 1 << 8, 1 << 16, 1 << 20}
	ls := []int64{1 << 20}
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false

	for _, nl := range ls {
		for _, r := range bools {
			log.Print("gmergesort strings test ", r)

			var l int = 32
			lns := randomdata.Randomstrings(nl, l, r)
			if len(lns) != int(nl) {
				log.Fatal("mergesort test randomdata lns: wanted len ", nl, " got ", len(lns))
			}

			slns := gmergesort(lns)

			if len(slns) != int(nl) {
				log.Fatal("mergesort test slns: wanted len ", nl, " got ", len(slns))
			}

			if !slices.IsSorted(slns) {
				t.Error("mergesort failed for size ", nl)
			} else {
				log.Print("mergesort test passed for ", nl)
			}
		}

	}

	for _, nl := range ls {
		log.Print("gmergesort uints test")

		lns := randomdata.Randomuints(nl)
		if len(lns) != int(nl) {
			log.Fatal("mergesort test randomdata lns: wanted len ", nl, " got ", len(lns))
		}

		slns := gmergesort(lns)

		if len(slns) != int(nl) {
			t.Fatal("mergesort test slns: wanted len ", nl, " got ", len(slns))
		}

		if !slices.IsSorted(slns) {
			t.Fatal("mergesort failed for size ", nl)
		} else {
			log.Print("mergesort test passed for ", nl)
		}

	}
}
