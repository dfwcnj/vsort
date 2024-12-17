package sorts

import (
	"log"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_splitbytesslice(t *testing.T) {
	var rlen int = 32
	var np int = 10
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	nrsa := []int64{1 << 20, 1 << 22, 1 << 24}

	for _, nrs := range nrsa {
		for _, r := range bools {
			log.Printf("sortflbytesfile test %v %v", nrs, r)

			ssl := randomdata.Randomstrings(nrs, rlen, r)

			bsl := make([][]byte, 0, nrs)
			for _, s := range ssl {
				bsl = append(bsl, []byte(s))
			}

			parts := splitbytesslice(bsl, np)
			log.Printf("splitbytesslice %v parts", len(parts))

			var nlns int64
			for i, part := range parts {
				pl := len(part)
				if r == false {
					for j, l := range part {
						if len(l) != rlen {
							t.Fatalf("splitbytesslice %v %v %v", i, j, l)
						}
					}
				}
				nlns += int64(pl)
			}
			log.Print("splitbytesslice after count")

			if nrs != nlns {
				t.Fatalf("splitbytesslice test wanted %v got %v", nrs, nlns)
			}
		}
	}
	log.Print("splitbytesslice test passed")
}
