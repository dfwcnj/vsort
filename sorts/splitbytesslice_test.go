package sorts

import (
	"log"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_splitbytesslice(t *testing.T) {
	var rlen int = 32
	var r bool = false
	nrsa := []int64{1 << 20, 1 << 22, 1 << 24}

	for _, nrs := range nrsa {
		log.Printf("sortflbytesfile test %v", nrs)

		ssl := randomdata.Randomstrings(nrs, rlen, r)

		bsl := make([][]byte, 0, nrs)
		for _, s := range ssl {
			bsl = append(bsl, []byte(s))
		}

		parts := splitbytesslice(bsl, 10)
		var np = len(parts)
		log.Printf("splitbytesslice %v parts", np)

		var nlns int64
		for i := range np {
			pl := len(parts[i])
			nlns += int64(pl)
		}
		log.Print("splitbytesslice after count")

		if nrs != nlns {
			t.Fatalf("splitbytesslice test wanted %v got %v", nrs, nlns)
		}
	}
	log.Print("splitbytesslice test passed")
}
