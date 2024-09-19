package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_kvsinsertionsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 16}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = false
			var e bool = false
			var keyoff = 0
			var reclen = ll
			var keylen = ll
			log.Print("testing kvsinsertionsort of ", nl, " random strings length ", l)
			lns := randomdata.Randomstrings(nl, l, r, e)
			if len(lns) != int(nl) {
				t.Fatal("kvsinsertionsort test lns: wanted len ", nl, " got ", len(lns))
			}
			kvsinsertionsort(lns, reclen, keyoff, keylen)
			if !slices.IsSorted(lns) {
				t.Fatal("kvsinsertionsort test failed not sorted")
			} else {
				log.Print("kvsinsertionsort test passed for ", nl)
			}

		}
	}
}
