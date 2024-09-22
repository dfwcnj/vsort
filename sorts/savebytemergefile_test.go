package sorts

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dfwcnj/vsort/merge"
	"github.com/dfwcnj/randomdata"
)

func Test_savebytemergefile(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20

	for _, r := range bools {

		log.Print("savebytemergefile test ", r)
		dn, err := initmergedir("/tmp", "savebytemergefiletest")
		if err != nil {
			log.Fatal("savebytemergefile test initmergedir ", err)
		}
		defer os.RemoveAll(dn)
		//log.Print("savemergeگile test initmergedir ", dn)

		for i := range 10 {
			var lns [][]byte
			rsl := randomdata.Randomstrings(nrs, rlen, r)
			for _, s := range rsl {
				ln := []byte(s)
				if r == true {
					ln = append(ln, "\n"...)
				}
				lns = append(lns, ln)
			}

			rsort2ba(lns)

			if len(lns) != int(nrs) {
				log.Fatal("savebytemergefile test lns: before sort wanted len ", rlen, " got ", len(lns))
			}

			var fn = filepath.Join(dn, fmt.Sprint("file", i))

			merge.Savebytemergefile(lns, fn)
			//log.Print("savebytemergefile test ", fn)

			var nl int64
			if r == true {
				nl = filelinecount(fn)
			} else {
				nl = filereccount(fn, rlen)
			}

			if nl != nrs {
				t.Fatal("savebytemergefile test failed rlns wanted ", nrs, " got ", nl)
			}
		}
	}
	log.Print("savebytemergefile test passed")
}
