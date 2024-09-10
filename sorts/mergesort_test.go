package sorts

import (
	"github.com/dfwcnj/randomdata"
	"log"
	"slices"
	"testing"
)

func Test_Mergesort(t *testing.T) {

	var r bool = true
	var e bool = false
	//ls := []int64{1, 2, 1 << 4, 1 << 8, 1 << 16, 1 << 20}
	ls := []int64{1 << 20}

	for _, nl := range ls {

		//log.Print("testing mergesort of ", nl, " random strings")
		var l int = 32
		lns := randomdata.Randomstrings(nl, l, r, e)
		if len(lns) != int(nl) {
			log.Fatal("randomdata lns: wanted len ", nl, " got ", len(lns))
		}

		slns := Mergesort(lns)

		if len(slns) != int(nl) {
			log.Fatal("mergesort slns: wanted len ", nl, " got ", len(slns))
		}

		if !slices.IsSorted(slns) {
			t.Error("mergesort failed for size ", nl)
		} else {
			log.Print("mergesort test passed for ", nl)
		}

	}

	for _, nl := range ls {

		//log.Print("testing mergesort of ", nl, " random uints")
		lns := randomdata.Randomuints(nl, e)
		if len(lns) != int(nl) {
			log.Fatal("randomdata lns: wanted len ", nl, " got ", len(lns))
		}

		slns := Mergesort(lns)

		if len(slns) != int(nl) {
			log.Fatal("mergesort slns: wanted len ", nl, " got ", len(slns))
		}

		if !slices.IsSorted(slns) {
			t.Error("mergesort failed for size ", nl)
		} else {
			log.Print("mergesort test passed for ", nl)
		}

	}
}
