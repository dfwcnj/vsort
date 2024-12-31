package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_kvbinsertionsort(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 17}

	for _, ll := range ls {
		for _, nl := range ns {
			log.Printf("kvbinsertionsort test %v %v", ll, nl)

			var l int = ll
			var r bool = false
			var keyoff = 0
			var reclen = ll
			var keylen = ll
			rsl := randomdata.Randomstrings(nl, l, r)
			if len(rsl) != int(nl) {
				t.Fatal("kvbinsertionsort test rsl: wanted len ", nl, " got ", len(rsl))
			}
			lns := make([][]byte, 0, nl)
			for _, s := range rsl {
				lns = append(lns, []byte(s))
			}

			t0 := time.Now()
			kvbinsertionsort(lns, reclen, keyoff, keylen)
			log.Printf("kvbinsertionsort test duration %v", time.Since(t0))

			ssl := make([]string, 0, nl)
			for _, bs := range lns {
				ssl = append(ssl, string(bs))
			}
			if !slices.IsSorted(ssl) {
				t.Fatal("kvbinsertionsort test failed not sorted")
			}
		}
	}
	log.Print("kvbinsertionsort test passed")
}
