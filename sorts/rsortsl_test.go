package sorts

import (
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_rsortsl(t *testing.T) {

	ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ns := []int64{1 << 3, 1 << 16, 1 << 20, 1 << 24}

	for _, ll := range ls {
		for _, nl := range ns {

			var lns [][]byte
			var l int = ll
			var r bool = true
			var e bool = false
			log.Print("testing rsortsl of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				log.Fatal("rsortsl test rsl: wanted len ", nl, " got ", len(rsl))
			}
			for _, s := range rsl {
				bln := []byte(s)
				lns = append(lns, bln)
			}
			if len(lns) != int(nl) {
				log.Print(lns)
				log.Fatal("rsortsl test lns: before rsortsl wanted len ", nl, " got ", len(lns))
			}
			slns := rsortsl(lns, 0)
			if len(slns) != int(nl) {
				//log.Print(ulns)
				log.Fatal("rsortsl test ulns: after rsortsl wanted len ", nl, " got ", len(slns))
			}
			var ssl []string
			for _, s := range slns {
				ssl = append(ssl, string(s))
			}

			if !slices.IsSorted(ssl) {
				log.Fatal("rsortsl test failed for size ", nl)
			} else {
				log.Print("rsortsl test passed for ", nl)
			}

		}
	}
}
