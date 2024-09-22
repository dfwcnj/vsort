package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_ginsertionsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	ns := []int64{1 << 16}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = true
			//log.Print("testing ginsertionsort of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r)
			if len(rsl) != int(nl) {
				t.Fatal("ginsertionsort test rsl: wanted len ", nl, " got ", len(rsl))
			}
			ginsertionsort(rsl)
			if !slices.IsSorted(rsl) {
				t.Fatal("ginsertionsort test failed for size ", nl)
			} else {
				log.Print("ginsertionsort test passed for ", nl)
			}

		}
	}
}
