package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_gheapsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 23}
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true

	for _, ll := range ls {
		for _, nl := range ns {
			for _, r := range bools {
				log.Print("gheapsort test ", r)

				var l int = ll
				//log.Print("gheapsort test of ", nl, " random strings length ", l)
				rsl := randomdata.Randomstrings(nl, l, r)
				if len(rsl) != int(nl) {
					t.Fatal("gheapsort test rsl: wanted len ", nl, " got ", len(rsl))
				}
				t0 := time.Now()
				gheapsort(rsl)
				log.Printf("gheapsort test duration %v", time.Since(t0))
				if !slices.IsSorted(rsl) {
					t.Fatal("gheapsort test failed not sorted")
				} else {
					log.Print("gheapsort test passed for ", nl)
				}
			}

		}
	}
}
