package sorts

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
)

func Test_savemergefile(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var e bool = false
	var nrs int64 = 1 << 20

	for _, r := range bools {

		log.Print("savemergefile test ", r)
		dn, err := initmergedir("/tmp", "savemergefiletest")
		if err != nil {
			log.Fatal("savemergefile test initmergedir ", err)
		}
		defer os.RemoveAll(dn)
		//log.Print("savemergeگile test initmergedir ", dn)

		for i := range 10 {
			var lns [][]byte
			rsl := randomdata.Randomstrings(nrs, rlen, r, e)
			for _, s := range rsl {
				ln := []byte(s)
				if r == true {
					ln = append(ln, "\n"...)
				}
				lns = append(lns, ln)
			}

			rsort2a(lns)

			if len(lns) != int(nrs) {
				log.Fatal("savemergefile test lns: before sort wanted len ", rlen, " got ", len(lns))
			}

			var fn = filepath.Join(dn, fmt.Sprint("file", i))

			merge.Savemergefile(lns, fn)
			//log.Print("savemergefile test ", fn)

			var nl int64
			if r == true {
				nl = filelinecount(fn)
			} else {
				nl = filereccount(fn, rlen)
			}

			if nl != nrs {
				t.Fatal("savemergefile test failed rlns wanted ", nrs, " got ", nl)
			}
		}
	}
	log.Print("savemergefile test passed")
}
