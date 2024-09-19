package sorts

import (
	"bufio"
	"log"
	"os"
	"path"
	"slices"
	"testing"

	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
)

func Test_sortflbytesfile(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)

	var lns [][]byte
	var err error
	var nr int

	dn, err := initmergedir("/tmp", "sortflbytesfiletest")
	if err != nil {
		log.Fatal("sortflbytesfile test initmergedir ", err)
	}
	//log.Print("sortflbytesfile test initmergedir ", dn)

	log.Println("sortflbytesfile test")

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "sortflbytesfiletest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer fp.Close()
	nw := bufio.NewWriter(fp)
	if err != nil {
		log.Fatal("sortflbytesfile test NewWriter ", err)
	}
	rlns := make([][]byte, 0, nrs)
	for _, s := range rsl {
		rlns = append(rlns, []byte(s))
	}
	for i, _ := range rlns {
		_, err := nw.Write(rlns[i])
		if err != nil {
			log.Fatal("sortflbytesfile test Write ", err)
		}
		nr++
	}
	nw.Flush()
	fp.Close()
	//log.Print("sortflbytesfile test file ", fn)

	lns, fns, err := sortflbytesfile(fn, dn, "std", iomem)
	if len(lns) != 0 {
		log.Fatal("sortflbytesfile test lns ", len(lns))
	}

	//log.Println("sortflbytesfile test after fns ", fns, " ", err)

	var nss int64
	for _, f := range fns {
		mfp, err := os.Open(f)
		if err != nil {
			log.Fatal("sortflbytesfile test open ", err)
		}
		finf, err := mfp.Stat()
		lns, _, err = merge.Flreadbytes(mfp, 0, finf.Size())
		//log.Println("sortflbytesfile test lns ", len(lns))

		var slns = make([]string, 0)
		for _, l := range lns {
			slns = append(slns, string(l))
		}
		if slices.IsSorted(slns) == false {
			t.Fatal("sortflbytesfile test failed  ", f, " is not sorted")
		}
		nss += int64(len(slns))
	}
	if nrs != nss {
		t.Fatal("sortflbytesfile failed test wanted ", nrs, " got ", nss)
	}
	log.Print("sortflbytesfile test passed")
}
