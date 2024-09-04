package sorts

import (
	"log"
	"os"
	"slices"
	"testing"

	"github.com/dfwcnj/govbinsort/dtypes"
	"github.com/dfwcnj/randomdata"
)

func Test_klslicessort(t *testing.T) {

	log.Print("klslicessort test")
	var l int = 32
	var r bool = false
	var e bool = false
	//ls := []int64{1, 2, 1 << 4, 1 << 8, 1 << 16, 1 << 20, 1 << 24}
	ls := []int64{1 << 4, 1 << 10, 1 << 20, 1 << 24}

	for _, nl := range ls {

		log.Print("klslicessort test ", nl)
		var klns kvallines

		//log.Print("testing sort of ", nl)
		rsl := randomdata.Randomstrings(nl, l, r, e)
		if len(rsl) != int(nl) {
			log.Fatal("rsl: wanted len ", nl, " got ", len(rsl))
		}
		for _, s := range rsl {
			var kln kvalline
			bln := []byte(s)
			kln.line = bln
			kln.key = kln.line[8:24]
			klns = append(klns, kln)
		}
		if len(klns) != int(nl) {
			log.Fatal("klns: before sort wanted len ", nl, " got ", len(klns))
		}
		Klslicessort(klns)

		var ssl []string
		for _, s := range klns {
			ssl = append(ssl, string(s.key))
		}
		if len(ssl) != 1 && ssl[0] == ssl[len(ssl)-1] {
			log.Fatal("strings are all equal")
		}
		if len(ssl) != int(nl) {
			log.Fatal("klslicessort test ssl: wanted len ", nl, " got ", len(ssl))
		}
		if !slices.IsSorted(ssl) {
			fp, err := os.OpenFile("/tmp/klslicessort", os.O_RDWR|os.O_CREATE, 0600)
			if err != nil {
				log.Fatal(err)
			}
			for _, l := range ssl {
				l = l + "\n"
				fp.Write([]byte(l))
			}
			fp.Close()
			log.Fatal("klslicessort test not in sort order")
		} else {
			log.Print("klslicessort test passed")
		}
	}
}
