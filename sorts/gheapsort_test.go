package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_gheapsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 20}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = true
			var e bool = false
			//log.Print("testing gheapsort of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				t.Fatal("gheapsort test rsl: wanted len ", nl, " got ", len(rsl))
			}
			gheapsort(rsl)
			if !slices.IsSorted(rsl) {
				t.Fatal("gheapsort test failed not sorted")
			} else {
				log.Print("gheapsort test passed for ", nl)
			}

		}
	}
}
