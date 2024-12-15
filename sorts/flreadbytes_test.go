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

func Test_flreadbytes(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var nrs int64 = 1 << 21
	var iomem int64 = nrs * 8

	var err error
	var nr int

	log.Print("flreadbytes test")

	slns := randomdata.Randomstrings(nrs, rlen, r)
	// log.Print("flreadbytes test lns ", len(lns))

	dn, err := initmergedir("/tmp", "flreadbytestest")
	if err != nil {
		t.Fatal("flreadbytes test initmergedir ", err)
	}
	// log.Print("flreadbytes initmergedir ", dn)
	defer os.RemoveAll(dn)

	fn := path.Join(dn, "flreadbytestest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal("flreadbytes test open ", err)
	}
	defer fp.Close()

	for i := range slns {
		fmt.Fprint(fp, slns[i])
		nr++
	}
	fp.Sync()
	// finf, err := fp.Stat()
	// log.Printf("flreadbytes test %v size %v", fn, finf.Size())
	fp.Close()

	var offset int64
	var tlns [][]byte
	var tnrs int64
	fp, err = os.Open(fn)
	if err != nil {
		t.Fatalf("flreadbytes test %v open %v", fn, err)
	}
	defer fp.Close()
	for {
		// log.Printf("flreadbytes test flreadbytes %v offset %v iomem %v", fn, offset, iomem)
		tlns, offset, err = merge.Flreadbytes(fp, offset, int(rlen), iomem)
		if err != nil && err != io.EOF {
			t.Fatalf("flreadbytes test %v %v", fn, err)
		}
		if len(tlns) == 0 {
			break
		}
		for i := range tlns {
			if len(tlns[i]) != int(rlen) {
				t.Fatalf("flreadbytes test failed  %v len %v", tlns[i], len(tlns[i]))
			}
			//log.Print(string(tlns[i]))
		}
		tnrs += int64(len(tlns))
		if err == io.EOF {
			break
		}
	}
	if tnrs != nrs {
		t.Fatalf("flreadbytes test failed  expected %v got %v", nrs, tnrs)
	}
	log.Print("flreadbytes test passed")
}
