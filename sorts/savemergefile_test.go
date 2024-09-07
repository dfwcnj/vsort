package sorts

import (
	"bufio"
	"fmt"
	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_savemergefile(t *testing.T) {
	var l int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	var dlim string
	dlim = "\n"

	log.Print("savemergefile test")
	dn, err := merge.Initmergedir("/tmp", "somesort")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dn)

	for i := range 10 {
		var lns [][]byte
		var ln []byte

		rsl := randomdata.Randomstrings(nrs, l, r, e)
		for _, s := range rsl {
			ln := []byte(s)
			lns = append(lns, ln)
		}
		if len(kns) != int(nrs) {
			//log.Print(klns)
			log.Fatal("klns: before sort wanted len ", rlen, " got ", len(lns))
		}

		slns := doslicessort(lns, 0, 0, 0, 0)
		var fn = filepath.Join(dn, fmt.Sprint("file", i))
		savemergefile(slns, fn, dlim)

		fp, err := os.Open(fn)
		if err != nil {
			log.Fatal(err)
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
			log.Fatal("rlns wanted ", nrs, " got ", len(rlns))
		}
	}
	log.Print("savemergefile test passed")
}
