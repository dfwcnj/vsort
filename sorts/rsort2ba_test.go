package sorts

import (
	"encoding/binary"
	"log"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_rsort2ba(t *testing.T) {

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
			//log.Print("testing rsort2ba of ", nl, " random strings length ", l)
			rsl := randomdata.Randomstrings(nl, l, r, e)
			if len(rsl) != int(nl) {
				log.Fatal("rsort2ba test rsl: wanted len ", nl, " got ", len(rsl))
			}
			for _, s := range rsl {
				bln := []byte(s)
				lns = append(lns, bln)
			}
			if len(lns) != int(nl) {
				log.Fatal("rsort2ba test lns: before rsort2ba wanted len ", nl, " got ", len(lns))
			}
			rsort2ba(lns)
			if len(lns) != int(nl) {
				log.Fatal("rsort2ba test ulns: after rsort2ba wanted len ", nl, " got ", len(lns))
			}
			var ssl []string
			for _, s := range lns {
				ssl = append(ssl, string(s))
			}

			if !slices.IsSorted(ssl) {
				t.Error("rsort2ba test failed for size ", nl)
			} else {
				log.Print("rsort2ba test passed for ", nl)
			}

			//log.Print("testing rsort2ba of ", nl, " random uints")
			ulns := randomdata.Randomuints(nl, e)
			if len(ulns) != int(nl) {
				log.Fatal("rsort2ba test rui: wanted len ", nl, " got ", len(lns))
			}
			lns = lns[:0]
			ub := make([]byte, 8)
			for _, u := range ulns {
				binary.LittleEndian.PutUint64(ub, u)
				lns = append(lns, ub)
			}
			rsort2ba(lns)
			if len(lns) != int(nl) {
				log.Fatal("rsort2ba test ulns: after rsort2ba wanted len ", nl, " got ", len(lns))
			}
			ulns = ulns[:0]
			for _, s := range lns {
				ui := binary.BigEndian.Uint64(s)
				ulns = append(ulns, ui)
			}

			if len(ulns) != int(nl) {
				t.Fatal("rsort2ba test ssl: wanted len ", nl, " got ", len(ulns))
			}
			if !slices.IsSorted(ulns) {
				t.Fatal("rsort2ba test failed for size ", nl)
			} else {
				log.Print("rsort2ba test passed for ", nl)
			}

		}
	}
}
