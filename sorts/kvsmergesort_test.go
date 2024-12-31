package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_kvsmergesort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 23}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = false
			var keyoff = 0
			var reclen = ll
			var keylen = ll
			log.Printf("kvsmergesort test %v %v %v", r, l, reclen)
			rsl := randomdata.Randomstrings(nl, l, r)
			if len(rsl) != int(nl) {
				t.Fatal("kvsmergesort test rsl: wanted len ", nl, " got ", len(rsl))
			}

			t0 := time.Now()
			slns := kvsmergesort(rsl, reclen, keyoff, keylen)
			log.Printf("kvsmergesort test duration %v", time.Since(t0))

			if len(slns) != int(nl) {
				t.Fatal("kvsmergesort test rsl: wanted len ", nl, " got ", len(slns))
			}
			log.Print("kvsmergesort sorted")
			if !slices.IsSorted(slns) {
				t.Fatal("kvsmergesort test failed not sorted")
			}
		}
	}
	log.Print("kvsmergesort test passed")
}
