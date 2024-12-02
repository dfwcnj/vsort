package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_kvslicesssort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 20}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = false
			var keyoff = 0
			var reclen = ll
			var keylen = ll
			//log.Print("testing kvslicesssort of ", nl, " random strings length ", l)
			lns := randomdata.Randomstrings(nl, l, r)
			if len(lns) != int(nl) {
				t.Fatal("kvslicesssort test lns: wanted len ", nl, " got ", len(lns))
			}
			kvslicesssort(lns, reclen, keyoff, keylen)
			if !slices.IsSorted(lns) {
				t.Fatal("kvslicesssort test failed not sorted")
			}
		}
	}
	log.Print("kvslicesssort test passed")
}
