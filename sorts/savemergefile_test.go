package sorts

import (
	"bufio"
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
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	dlim := "\n"

	//log.Print("savemergefile test")
	dn, err := initmergedir("/tmp", "savemergefiletest")
	if err != nil {
		log.Fatal("savemergefile test initmergedir ", err)
	}
	defer os.RemoveAll(dn)

	for i := range 10 {
		var lns [][]byte

		rsl := randomdata.Randomstrings(nrs, rlen, r, e)
		for _, s := range rsl {
			ln := []byte(s)
			lns = append(lns, ln)
		}

		rsort2a(lns)

		if len(lns) != int(nrs) {
			log.Fatal("savemergefile test lns: before sort wanted len ", rlen, " got ", len(lns))
		}

		var fn = filepath.Join(dn, fmt.Sprint("file", i))
		merge.Savemergefile(lns, fn, dlim)
		//log.Print("savemergefile test Savemergefile returned ", mf)

		fp, err := os.Open(fn)
		if err != nil {
			log.Fatal("savemergefile test open ", err)
		}
		defer fp.Close()

		scanner := bufio.NewScanner(fp)
		var rlns []string
		for scanner.Scan() {
			l := scanner.Text()
			if len(l) == 0 {
				continue
			}
			rlns = append(rlns, l)
		}
		if len(rlns) != int(nrs) {
			t.Fatal("savemergefile test failed rlns wanted ", nrs, " got ", len(rlns))
		}
	}
	log.Print("savemergefile test passed")
}
