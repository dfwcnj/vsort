package sorts

import (
	"encoding/binary"
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_rsort2sa(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 20}

	for _, ll := range ls {
		for _, nl := range ns {

			var lns [][]byte
			var l int = ll
			var r bool = true
			var e bool = false
			//log.Print("testing rsort2sa of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				log.Fatal("rsort2sa test rsl: wanted len ", nl, " got ", len(rsl))
			}
			for _, s := range rsl {
				bln := []byte(s)
				lns = append(lns, bln)
			}
			if len(lns) != int(nl) {
				log.Fatal("rsort2sa test lns: before rsort2sa wanted len ", nl, " got ", len(lns))
			}
			rsort2sa(lns)
			if len(lns) != int(nl) {
				log.Fatal("rsort2sa test ulns: after rsort2sa wanted len ", nl, " got ", len(lns))
			}
			var ssl []string
			for _, s := range lns {
				ssl = append(ssl, string(s))
			}

			if !slices.IsSorted(ssl) {
				t.Error("rsort2sa test failed for size ", nl)
			} else {
				log.Print("rsort2sa test passed for ", nl)
			}

		}
	}
}
