package sorts

import (
	"fmt"
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
	var iomem int64 = 1<<24 + 1<<20
	var mrlen int

	var lns [][]byte
	var err error
	var nr int

	dn, err := merge.Initmergedir("/tmp", "somesort")

	log.Println("sortflrecfile test")

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "sortflrecfiletest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}

	for i, _ := range rsl {
		fmt.Fprint(fp, rsl[i])
		nr++
	}
	fp.Close()

	_, fns, mrlen, err := dosortflrecfile(fn, dn, "std", rlen, 0, 0, iomem)

	var nss int
	for _, f := range fns {
		mfp, err := os.Open(f)
		if err != nil {
			log.Fatal(err)
		}
		finf, err := mfp.Stat()
		if err != nil {
			log.Fatal("sortflrecfiletest ", err)
		}
		slns, _, err = Flreadn(mfp, 0, rlen, 0, 0, finf.Size())
		var lns = make([]string, 0)
		for _, t := range slns {
			lns = append(lns, string(t.line))
		}
		if slices.IsSorted(lns) == false {
			log.Fatal(f, " is not sorted")
		}
		nss += int(len(lns))
	}
	if nrs != int64(nss) {
		log.Fatal("sortflrecfile test wanted ", nrs, " got ", nss)
	}
	log.Println("sortflrecfile passed")

}
