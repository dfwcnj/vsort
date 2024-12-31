package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_kvsinsertionsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 17}

	for _, ll := range ls {
		for _, nl := range ns {

			var l int = ll
			var r bool = false
			var keyoff = 0
			var reclen = ll
			var keylen = ll
			// log.Printf("kvsinsertionsort %v %v", nl, l)
			lns := randomdata.Randomstrings(nl, l, r)
			if len(lns) != int(nl) {
				t.Fatal("kvsinsertionsort test lns: wanted len ", nl, " got ", len(lns))
			}

			t0 := time.Now()
			kvsinsertionsort(lns, reclen, keyoff, keylen)
			log.Printf("kvsinsertionsort duration %v", time.Since(t0))

			if !slices.IsSorted(lns) {
				t.Fatal("kvsinsertionsort test failed not sorted")
			}
		}
	}
	log.Print("kvsinsertionsort test passed")
}
