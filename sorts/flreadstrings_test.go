package sorts

import (
	"fmt"
	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
	"log"
	"os"
	"path"
	"testing"
)

func Test_flreadstrings(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)

	var lns []string
	var tlns []string
	var offset int64
	var err error
	var nr int

	// log.Print("flreadstrings test")

	rsl := randomdata.Randomstrings(nrs, rlen, r)
	// log.Print("flreadstrings test rsl ", len(rsl))

	dn, err := initmergedir("/tmp", "flreadstringstest")
	if err != nil {
		log.Fatal("flreadstrings test initmergedir ", err)
	}
	// log.Print("flreadstrings initmergedir ", dn)
	defer os.RemoveAll(dn)

	fn := path.Join(dn, "flreadstringstest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal("flreadstrings test open ", err)
	}
	defer fp.Close()

	for i, _ := range rsl {
		fmt.Fprint(fp, rsl[i])
		nr++
	}
	fp.Sync()

	// file length
	offset, err = fp.Seek(0, 1)
	if err != nil {
		log.Fatal("flreadstrings test seek 1 ", err)
	}

	// rewind file
	offset, err = fp.Seek(0, 0)
	if err != nil {
		log.Fatal("flreadstrings test seek 0 ", err)
	}

	for {
		//log.Println("flreadstrings test flreadstrings ", fn, " ", l)
		lns, offset, err = merge.Flreadstrings(fp, offset, int(rlen), iomem)
		if len(lns) == 0 {
			break
		}
		for _, ln := range lns {
			if len(ln) != int(rlen) {
				log.Fatal("ln ", ln, " len ", len(ln))
			}
			//log.Print(string(ln))
		}
		tlns = append(tlns, lns...)
	}
	if len(tlns) != int(nrs) {
		t.Fatal("flreadstrings failed  expected ", nrs, " got ", len(lns))
	}
	log.Print("flreadstrings test passed")
}
