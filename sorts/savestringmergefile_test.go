package sorts

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_savestringmergefile(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	var nrs int64 = 1 << 20

	for _, r := range bools {

		log.Print("savestringmergefile test ", r)
		dn, err := initmergedir("/tmp", "savestringmergefiletest")
		if err != nil {
			log.Fatal("savestringmergefile test initmergedir ", err)
		}
		defer os.RemoveAll(dn)
		//log.Print("savemergeگile test initmergedir ", dn)

		for i := range 10 {
			lns := randomdata.Randomstrings(nrs, rlen, r)
			// random length strings must be newline delimited
			if r == true {
				var nlns = len(lns)
				for i := range nlns {
					lns[i] = lns[i] + "\n"
				}
			}

			rsort2sa(lns, 0, 0, 0)

			if len(lns) != int(nrs) {
				log.Fatal("savestringmergefile test lns: before sort wanted len ", nrs, " got ", len(lns))
			}

			var fn = filepath.Join(dn, fmt.Sprint("file", i))

			merge.Savestringmergefile(lns, fn)
			//log.Print("savestringmergefile test ", fn)

			var nl int64
			if r == true {
				nl = filelinecount(fn)
			} else {
				nl = filereccount(fn, rlen)
			}

			if nl != nrs {
				t.Fatal("savestringmergefile test failed lns wanted ", nrs, " got ", nl)
			}
		}
	}
	log.Print("savestringmergefile test passed")
}
