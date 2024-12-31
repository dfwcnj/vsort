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

func Test_vlreadbytes(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var nrs int64 = 1 << 21
	var iomem int64 = nrs * 8
	var nr int

	log.Print("vlreadbytes test ")

	lns := randomdata.Randomstrings(nrs, rlen, r)

	dn, err := initmergedir("/tmp", "vlreadbytestest")
	if err != nil {
		t.Fatal("vlreadbytes test initmergedir ", err)
	}
	// log.Print("vlreadbytes test initmergedir ", dn)
	// defer os.RemoveAll(dn)

	fn := path.Join(dn, "vlreadbytes")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal("vlreadbytes test open ", err)
	}
	defer fp.Close()
	// log.Print("vlreadbytes test file ", fn)

	nw := bufio.NewWriter(fp)
	for _, l := range lns {
		_, err := nw.Write([]byte(l + "\n"))
		if err != nil {
			t.Fatal("vlreadbytes test write ", err)
		}
		nr++
	}
	err = nw.Flush()
	if err != nil {
		t.Fatal("vlreadbytes test flush ", err)
	}
	// finf, _ := fp.Stat()
	// log.Printf("vlreadbytes test %v size %v", fn, finf.Size())
	fp.Close()

	var offset int64
	var tnrs int64
	var tlns [][]byte
	fp, err = os.Open(fn)
	if err != nil {
		t.Fatalf("vlreadbytes test %v open %v", fn, err)
	}
	defer fp.Close()
	for {
		// log.Printf("vlreadbytes test vlreadbytes %v offset %v", fn, offset)
		tlns, offset, err = merge.Vlreadbytes(fp, offset, iomem)
		if err != nil && err != io.EOF {
			t.Fatalf("flreadbytes test %v %v", fn, err)
		}
		if len(tlns) == 0 {
			break
		}
		for i := range tlns {
			if len(tlns[i]) == 0 {
				t.Fatalf("vlreadbytes test failed %v len %v", tlns[i], 0)
			}
			//log.Print(string(tlns[i]))
		}
		tnrs += int64(len(tlns))
		if err == io.EOF {
			break
		}
	}
	if tnrs != nrs {
		t.Fatalf("vlreadbytes test failed expected %v got %v", nrs, tnrs)
	}
	log.Print("vlreadbytes test passed")
}
