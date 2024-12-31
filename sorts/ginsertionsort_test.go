package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_ginsertionsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	ns := []int64{1 << 17}
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true

	for _, ll := range ls {
		for _, nl := range ns {
			for _, r := range bools {
				log.Print("ginsertionsort test ", r)

				var l int = ll
				//log.Print("testing ginsertionsort of ", nl, " random strings length ", l)
				rsl := randomdata.Randomstrings(nl, l, r)
				if len(rsl) != int(nl) {
					t.Fatal("ginsertionsort test rsl: wanted len ", nl, " got ", len(rsl))
				}
				t0 := time.Now()
				ginsertionsort(rsl)
				log.Printf("ginsertionsort test duration %v", time.Since(t0))
				if !slices.IsSorted(rsl) {
					t.Fatal("ginsertionsort test failed for size ", nl)
				} else {
					log.Print("ginsertionsort test passed for ", nl)
				}
			}

		}
	}
}
