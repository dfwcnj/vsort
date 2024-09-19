package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_kvbheapsort(t *testing.T) {

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
			log.Print("testing kvbheapsort of ", nl, " strings length ", ll)
			rsl := randomdata.Randomstrings(nl, ll, r, e)
			if len(rsl) != int(nl) {
				t.Fatal("kvbheapsort test rsl: wanted len ", nl, " got ", len(rsl))
			}
			lns := make([][]byte, 0, nl)
			for _, s := range rsl {
				lns = append(lns, []byte(s))
			}
			kvbheapsort(lns, reclen, keyoff, keylen)
			ssl := make([]string, 0, nl)
			for _, bs := range lns {
				ssl = append(ssl, string(bs))
			}
			if !slices.IsSorted(ssl) {
				for _, s := range ssl {
					log.Print(s)
				}
				t.Fatal("kvbheapsort test failed not sorted")
			} else {
				log.Print("kvbheapsort test passed for ", nl)
			}

		}
	}
}
