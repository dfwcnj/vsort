package sorts

import (
	"bufio"
	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
	"log"
	"os"
	"path"
	"slices"
	"testing"
)

func Test_sortflrecfile(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)

	var err error
	var nr int

	dn, err := initmergedir("/tmp", "sortflrecfiletest")
	if err != nil {
		log.Fatal("sortflrecfile test initmergedir ", err)
	}
	//log.Print("sortflrecfile initmergedir ", dn)

	//log.Print("sortflrecfile test")

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "sortflrecfiletest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	nw := bufio.NewWriter(fp)
	if err != nil {
		log.Fatal("sortflrecfile test open ", err)
	}

	for i, _ := range rsl {
		nw.WriteString(rsl[i])
		nr++
	}
	nw.Flush()
	fp.Close()
	//log.Print("sortflrecfile test ", fn)

	lns, fns, err := sortflrecfile(fn, dn, "std", rlen, 0, 0, iomem)
	if len(lns) != 0 {
		log.Fatal("sortflrecfile test lns ", len(lns))
	}
	//log.Print("sortflrecfile test fns ", fns)

	var nss int
	for _, f := range fns {
		mfp, err := os.Open(f)
		if err != nil {
			log.Fatal("sortflrecfile test open ", err)
		}
		finf, err := mfp.Stat()
		if err != nil {
			log.Fatal("sortflrecfiletest ", err)
		}
		slns, _, err := merge.Flreadn(mfp, 0, rlen, finf.Size())
		var lns = make([]string, 0)
		for _, s := range slns {
			lns = append(lns, string(s))
		}
		if slices.IsSorted(lns) == false {
			t.Fatal("sortflrecfile test ", f, " is not sorted")
		}
		nss += int(len(lns))
	}
	if nrs != int64(nss) {
		t.Fatal("sortflrecfile test wanted ", nrs, " got ", nss)
	}
	log.Print("sortflrecfile passed")

}
