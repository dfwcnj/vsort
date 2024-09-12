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

func Test_sortvlrecfile(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)

	var lns [][]byte
	var err error
	var nr int

	dn, err := initmergedir("/tmp", "somesort")

	//log.Println("sortvlrecfile test")

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "sortvlrecfiletest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer fp.Close()
	nw := bufio.NewWriter(fp)
	if err != nil {
		log.Fatal("sortvlrecfile test NewWriter ", err)
	}
	for i, _ := range rsl {
		_, err := nw.WriteString(rsl[i] + "\n")
		if err != nil {
			log.Fatal("sortvlrecfile test WriteString ", err)
		}
		nr++
	}
	nw.Flush()
	fp.Close()
	//log.Print("sortvlrecfile test file ", fn)

	lns, fns, err := sortvlrecfile(fn, dn, "std", iomem)
	if len(lns) != 0 {
		log.Fatal("sortvlrecfile test lns ", len(lns))
	}

	//log.Println("sortvlrecfile test after fns ", fns, " ", err)

	var nss int64
	for _, f := range fns {
		mfp, err := os.Open(f)
		if err != nil {
			log.Fatal("sortvlrecfile test open ", err)
		}
		finf, err := mfp.Stat()
		lns, _, err = merge.Vlreadn(mfp, 0, finf.Size())
		//log.Println("sortvlrecfile test lns ", len(lns))

		var slns = make([]string, 0)
		for _, l := range lns {
			slns = append(slns, string(l))
		}
		if slices.IsSorted(slns) == false {
			t.Error("sortvlrecfile test ", f, " is not sorted")
		}
		nss += int64(len(slns))
	}
	if nrs != nss {
		t.Error("sortvlrecfile test wanted ", nrs, " got ", nss)
	}
	log.Print("sortvlrecfile test passed")
}
