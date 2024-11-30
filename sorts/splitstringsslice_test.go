package sorts

import (
	"log"
	"testing"

	"github.com/dfwcnj/randomdata"
)

func Test_splitstringsslice(t *testing.T) {
	var rlen int = 32
	var r bool = false
	nrsa := []int64{1 << 20, 1 << 22, 1 << 24}

	for _, nrs := range nrsa {
		log.Printf("sortflbytesfile test %v", nrs)

		ssl := randomdata.Randomstrings(nrs, rlen, r)

		parts := splitstringsslice(ssl, 10)
		log.Printf("splitstringsslice %v parts", len(parts))

		var nlns int64
		for i := range len(parts) {
			pl := len(parts[i])
			nlns += int64(pl)
		}
		log.Print("splitstringsslice after count")

		if nrs != nlns {
			t.Fatalf("splitstringsslice test wanted %v got %v", nrs, nlns)
		}
	}
	log.Print("splitstringsslice test passed")
}
