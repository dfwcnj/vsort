package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_kvsheapsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 20}

	for _, ll := range ls {
		for _, nl := range ns {

			var r bool = false
			var e bool = false
			var keyoff = 0
			var reclen = ll
			var keylen = ll
			log.Print("testing kvsheapsort of ", nl, " strings length ", ll)
			lns := randomdata.Randomstrings(nl, ll, r, e)
			if len(lns) != int(nl) {
				t.Fatal("kvsheapsort test rsl: wanted len ", nl, " got ", len(lns))
			}
			kvsheapsort(lns, reclen, keyoff, keylen)
			if !slices.IsSorted(lns) {
				t.Fatal("kvsheapsort test failed not sorted")
			} else {
				log.Print("kvsheapsort test passed for ", nl)
			}

		}
	}
}