package sorts

import (
	"fmt"
	"github.com/dfwcnj/vsort/merge"
	"github.com/dfwcnj/randomdata"
	"log"
	"os"
	"path"
	"testing"
)

func Test_vlreadstrings(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen)
	var nr int

	var lns []string

	//log.Print("vlreadstrings test")

	dn, err := initmergedir("/tmp", "vlreadreadstringstest")
	if err != nil {
		log.Fatal("vlreadstrings test initmergedir ", err)
	}
	// log.Print("vlreadstrings test initmergedir ", dn)
	defer os.RemoveAll(dn)

	rsl := randomdata.Randomstrings(nrs, rlen, r)

	fn := path.Join(dn, "vlreadstrings")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("vlreadstrings test open ", err)
	}
	defer fp.Close()

	for _, l := range rsl {
		fmt.Fprintln(fp, l+"\n")
		nr++
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
	log.Print("vlreadstrings test passed")
}
