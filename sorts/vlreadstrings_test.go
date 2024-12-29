package sorts

import (
	"bufio"
	"io"
	"log"
	"os"
	"path"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_vlreadstrings(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var nrs int64 = 1 << 21
	var iomem int64 = nrs * 8
	var nr int

	var lns []string

	log.Print("vlreadstrings test")

	lns = randomdata.Randomstrings(nrs, rlen, r)

	dn, err := initmergedir("/tmp", "vlreadreadstringstest")
	if err != nil {
		t.Fatal("vlreadstrings test initmergedir ", err)
	}
	// log.Print("vlreadstrings test initmergedir ", dn)
	// defer os.RemoveAll(dn)

	fn := path.Join(dn, "vlreadstrings")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal("vlreadstrings test open ", err)
	}
	defer fp.Close()
	// log.Print("vlreadstrings test file ", fn)

	nw := bufio.NewWriter(fp)
	for _, l := range lns {
		_, err := nw.WriteString(l + "\n")
		if err != nil {
			t.Fatal("flreadstrings test write ", err)
		}
		nr++
	}
	err = nw.Flush()
	if err != nil {
		t.Fatal("flreadstrings test flush ", err)
	}
	finf, _ := fp.Stat()
	log.Printf("vlreadstrings test %v size %v", fn, finf.Size())
	fp.Close()

	var offset int64
	var tlns []string
	var tnrs int64
	fp, err = os.Open(fn)
	if err != nil {
		t.Fatalf("vlreadstrings test %v open %v", fn, err)
	}
	defer fp.Close()
	for {
		// log.Printf("vlreadstrings test vlreadstrings %v offset %v", fn, offset)
		tlns, offset, err = merge.Vlreadstrings(fp, offset, iomem)
		if err != nil && err != io.EOF {
			t.Fatalf("flreadstrings test %v %v", fn, err)
		}
		if len(tlns) == 0 {
			break
		}
		for i := range tlns {
			if len(tlns[i]) == 0 {
				t.Fatalf("vlreadstrings test failed %v len %v", tlns[i], len(tlns[i]))
			}
			//log.Print(tlns[i])
		}
		tnrs += int64(len(tlns))
		if err == io.EOF {
			break
		}
	}
	if tnrs != nrs {
		t.Fatalf("vlreadstrings test expected %v got %v", nrs, tnrs)
	}
	log.Print("vlreadstrings test passed")
}
