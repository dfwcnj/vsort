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

func Test_sortvlstringsfile(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)

	var lns [][]byte
	var err error
	var nr int

	dn, err := initmergedir("/tmp", "sortvlstringsfiletest")
	if err != nil {
		log.Fatal("sortvlstringsfile test initmergedir ", err)
	}
	//log.Print("sortvlstringsfile test initmergedir ", dn)

	//log.Println("sortvlstringsfile test")

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "sortvlstringsfiletest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer fp.Close()
	nw := bufio.NewWriter(fp)
	if err != nil {
		log.Fatal("sortvlstringsfile test NewWriter ", err)
	}
	for i, _ := range rsl {
		_, err := nw.WriteString(rsl[i] + "\n")
		if err != nil {
			log.Fatal("sortvlstringsfile test WriteString ", err)
		}
		nr++
	}
	nw.Flush()
	fp.Close()
	//log.Print("sortvlstringsfile test file ", fn)

	lns, fns, err := sortvlstringsfile(fn, dn, "std", iomem)
	if len(lns) != 0 {
		log.Fatal("sortvlstringsfile test lns ", len(lns))
	}

	//log.Println("sortvlstringsfile test after fns ", fns, " ", err)

	var nss int64
	for _, f := range fns {
		mfp, err := os.Open(f)
		if err != nil {
			log.Fatal("sortvlstringsfile test open ", err)
		}
		finf, err := mfp.Stat()
		lns, _, err = merge.Vlreadn(mfp, 0, finf.Size())
		//log.Println("sortvlstringsfile test lns ", len(lns))

		var slns = make([]string, 0)
		for _, l := range lns {
			slns = append(slns, string(l))
		}
		if slices.IsSorted(slns) == false {
			t.Fatal("sortvlstringsfile test failed  ", f, " is not sorted")
		}
		nss += int64(len(slns))
	}
	if nrs != nss {
		t.Fatal("sortvlstringsfile failed test wanted ", nrs, " got ", nss)
	}
	log.Print("sortvlstringsfile test passed")
}