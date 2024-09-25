package sorts

import (
	"bufio"
	"log"
	"os"
	"path"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_vlreadstrings(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs*int64(rlen) + nrs
	var nr int

	var lns []string

	for _, r := range bools {
		log.Print("vlreadstrings test ", r)

		dn, err := initmergedir("/tmp", "vlreadreadstringstest")
		if err != nil {
			log.Fatal("vlreadstrings test initmergedir ", err)
		}
		// log.Print("vlreadstrings test initmergedir ", dn)
		// defer os.RemoveAll(dn)

		fn := path.Join(dn, "vlreadstrings")
		fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal("vlreadstrings test open ", err)
		}
		defer fp.Close()
		// log.Print("vlreadstrings test file ", fn)

		rsl := randomdata.Randomstrings(nrs, rlen, r)
		nw := bufio.NewWriter(fp)
		for _, l := range rsl {
			_, err := nw.WriteString(l + "\n")
			if err != nil {
				log.Fatal("flreadstrings test write ", err)
			}
			nr++
		}
		err = nw.Flush()
		if err != nil {
			log.Fatal("flreadstrings test flush ", err)
		}

		_, err = fp.Seek(0, 0)
		if err != nil {
			log.Fatal("vlreadstrings test seek ", err)
		}
		lns, _, err = merge.Vlreadstrings(fp, int64(0), iomem)
		for _, ln := range lns {
			if len(ln) == 0 {
				t.Fatal("vlreadstrings test len(ln) == 0")
			}
			//log.Print(string(ln))
		}
		if len(lns) != int(nrs) {
			t.Fatal("vlreadstrings: expected ", nrs, " got ", len(lns))
		}
	}
	log.Print("vlreadstrings test passed")
}
