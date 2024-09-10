package sorts

import (
	"encoding/binary"
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_rsort2a(t *testing.T) {

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
			log.Print("testing rsort2a of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				log.Fatal("rsort2a test rsl: wanted len ", nl, " got ", len(rsl))
			}
			for _, s := range rsl {
				bln := []byte(s)
				lns = append(lns, bln)
			}
			if len(lns) != int(nl) {
				log.Print(lns)
				log.Fatal("rsort2a test lns: before rsort2a wanted len ", nl, " got ", len(lns))
			}
			rsort2a(lns)
			if len(lns) != int(nl) {
				//log.Print(ulns)
				log.Fatal("rsort2a test ulns: after rsort2a wanted len ", nl, " got ", len(lns))
			}
			var ssl []string
			for _, s := range lns {
				ssl = append(ssl, string(s))
			}

			if !slices.IsSorted(ssl) {
				log.Fatal("rsort2a test failed for size ", nl)
			} else {
				log.Print("rsort2a test passed for ", nl)
			}

			log.Print("testing rsort2a of ", nl, " random uints")
			ulns := randomdata.Randomuints(nl, e)
			if len(ulns) != int(nl) {
				log.Fatal("rsort2a test rui: wanted len ", nl, " got ", len(lns))
			}
			lns = lns[:0]
			ub := make([]byte, 8)
			for _, u := range ulns {
				binary.LittleEndian.PutUint64(ub, u)
				lns = append(lns, ub)
			}
			rsort2a(lns)
			if len(lns) != int(nl) {
				//log.Print(ulns)
				log.Fatal("rsort2a test ulns: after rsort2a wanted len ", nl, " got ", len(lns))
			}
			ulns = ulns[:0]
			for _, s := range lns {
				ui := binary.BigEndian.Uint64(s)
				ulns = append(ulns, ui)
			}

			if len(ulns) != int(nl) {
				log.Fatal("rsort2a test ssl: wanted len ", nl, " got ", len(ulns))
			}
			if !slices.IsSorted(ulns) {
				log.Fatal("rsort2a test failed for size ", nl)
			} else {
				log.Print("rsort2a test passed for ", nl)
			}

		}
	}
}
