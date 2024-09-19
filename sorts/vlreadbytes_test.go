package sorts

import (
	"fmt"
	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
	"log"
	"os"
	"path"
	"testing"
)

func Test_vlreadbytes(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen)
	var nr int

	var lns [][]byte

	//log.Print("vlreadbytes test")

	dn, err := initmergedir("/tmp", "vlreadbytestest")
	if err != nil {
		log.Fatal("vlreadbytes test initmergedir ", err)
	}
	log.Print("vlreadbytes test initmergedir ", dn)
	defer os.RemoveAll(dn)

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "vlreadbytes")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("vlreadbytes test open ", err)
	}
	defer fp.Close()

	for _, l := range rsl {
		fmt.Fprintln(fp, l+"\n")
		nr++
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
	log.Print("vlreadn test passed")
}
