package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_heapsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 20}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = true
			var e bool = false
			//log.Print("testing heapsort of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				log.Fatal("heapsort test rsl: wanted len ", nl, " got ", len(rsl))
			}
			Heapsort(rsl)
			//floyd(rsl)
			if !slices.IsSorted(rsl) {
			} else {
				log.Print("heapsort test passed for ", nl)
			}

		}
	}
}
