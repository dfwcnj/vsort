package sorts

import (
	"log"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_rsortsl(t *testing.T) {

	//ls := []int{1 << 3, 1 << 4, 1 << 5, 1 << 6}
	ls := []int{1 << 5}
	//ns := []int64{1 << 3, 1 << 16, 1 << 20}
	ns := []int64{1 << 23}

	for _, ll := range ls {
		for _, nl := range ns {

			var lns [][]byte
			var l int = ll
			var r bool = true
			//log.Print("testing rsortsl of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r)
			if len(rsl) != int(nl) {
				log.Fatal("rsortsl test rsl: wanted len ", nl, " got ", len(rsl))
			}
			for _, s := range rsl {
				bln := []byte(s)
				lns = append(lns, bln)
			}
			if len(lns) != int(nl) {
				log.Fatal("rsortsl test lns: before rsortsl wanted len ", nl, " got ", len(lns))
			}

			t0 := time.Now()
			slns := rsortsl(lns, 0)
			log.Printf("rsortsl test duration %v", time.Since(t0))

			if len(slns) != int(nl) {
				log.Fatal("rsortsl test ulns: after rsortsl wanted len ", nl, " got ", len(slns))
			}
			var ssl []string
			for _, s := range slns {
				ssl = append(ssl, string(s))
			}

			if !slices.IsSorted(ssl) {
				t.Fatal("rsortsl test failed for size ", nl)
			}

		}
	}
	log.Print("rsortsl test passed")
}
