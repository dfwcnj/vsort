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

func Test_vlreadbytes(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	var nrs int64 = 1 << 20
	var iomem int64 = nrs*int64(rlen) + nrs
	var nr int

	var lns [][]byte

	//log.Print("vlreadbytes test")

	for _, r := range bools {
		log.Print("vlreadbytes test ", r)
		dn, err := initmergedir("/tmp", "vlreadbytestest")
		if err != nil {
			log.Fatal("vlreadbytes test initmergedir ", err)
		}
		// log.Print("vlreadbytes test initmergedir ", dn)
		// defer os.RemoveAll(dn)

		fn := path.Join(dn, "vlreadbytes")
		fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal("vlreadbytes test open ", err)
		}
		defer fp.Close()
		// log.Print("vlreadbytes test file ", fn)

		nw := bufio.NewWriter(fp)
		rsl := randomdata.Randomstrings(nrs, rlen, r)
		for _, l := range rsl {
			_, err := nw.Write([]byte(l + "\n"))
			if err != nil {
				log.Fatal("vlreadbytes test write ", err)
			}
			nr++
		}
		err = nw.Flush()
		if err != nil {
			log.Fatal("vlreadbytes test flush ", err)
		}

		_, err = fp.Seek(0, 0)
		if err != nil {
			log.Fatal("vlreadbytes test seek ", err)
		}
		lns, _, err = merge.Vlreadbytes(fp, int64(0), iomem)
		for _, ln := range lns {
			if len(ln) == 0 {
				t.Fatal("vlreadbytes test len(ln) == 0")
			}
			//log.Print(string(ln))
		}
		if len(lns) != int(nrs) {
			t.Fatal("vlreadbytes: expected ", nrs, " got ", len(lns))
		}
	}
	log.Print("vlreadbytes test passed")
}
