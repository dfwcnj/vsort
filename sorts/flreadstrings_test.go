package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_flreadstrings(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var nrs int64 = 1 << 21
	var iomem int64 = nrs * 8

	var lns []string
	var err error
	var nr int

	log.Print("flreadstrings test")

	lns = randomdata.Randomstrings(nrs, rlen, r)
	// log.Print("flreadstrings test lns ", len(lns))

	dn, err := initmergedir("/tmp", "flreadstringstest")
	if err != nil {
		t.Fatal("flreadstrings test initmergedir ", err)
	}
	// log.Print("flreadstrings initmergedir ", dn)
	defer os.RemoveAll(dn)

	fn := path.Join(dn, "flreadstringstest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal("flreadstrings test open ", err)
	}
	defer fp.Close()

	for i := range lns {
		fmt.Fprint(fp, lns[i])
		nr++
	}
	fp.Sync()
	// finf, err := fp.Stat()
	// log.Printf("flreadstrings test %v size %v", fn, finf.Size())
	fp.Close()

	var offset int64
	var tlns []string
	var tnrs int64
	fp, err = os.Open(fn)
	if err != nil {
		t.Fatalf("flreadstrings test %v όpen %v", fn, err)
	}
	defer fp.Close()
	for {
		// log.Printf("flreadstrings test flreadstrings %v offset %v", fn, offset)
		tlns, offset, err = merge.Flreadstrings(fp, offset, int(rlen), iomem)
		if err != nil && err != io.EOF {
			t.Fatalf("flreadbytes test %v %v", fn, err)
		}
		if len(tlns) == 0 {
			break
		}
		for i := range tlns {
			if len(tlns[i]) != rlen {
				t.Fatalf("flreadstrings test failed  %v len %v", tlns[i], len(tlns[i]))
			}
			//log.Print(string(tlns[i]))
		}
		tnrs += int64(len(tlns))
		if err == io.EOF {
			break
		}
	}
	if tnrs != nrs {
		t.Fatalf("flreadstrings test failed expected %v got %v", nrs, tnrs)
	}
	log.Print("flreadstrings test passed")
}
