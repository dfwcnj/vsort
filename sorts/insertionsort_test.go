package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_insertionsort(t *testing.T) {

	ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ns := []int64{1 << 3, 1 << 16}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = true
			var e bool = false
			log.Print("testing insertionsort of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				log.Fatal("insertionsort test rsl: wanted len ", nl, " got ", len(rsl))
			}
			Insertionsort(rsl)
			if !slices.IsSorted(rsl) {
				log.Fatal("insertionsort test failed for size ", nl)
			} else {
				log.Print("insertionsort test passed for ", nl)
			}

		}
	}
}
