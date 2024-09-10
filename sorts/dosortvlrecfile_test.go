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

func Test_dosortvlrecfile(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	var nss int64
	var iomem int64 = nrs * int64(rlen / 2)

	var lns [][]byte
	var err error
	var nr int

	dn, err := initmergedir("/tmp", "somesort")

	log.Println("dosortvlrecfile test")

	rsl := randomdata.Randomstrings(nrs, rlen, r, e)

	fn := path.Join(dn, "dosortvlrecfiletest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer fp.Close()
	nw := bufio.NewWriter(fp)
	if err != nil {
		log.Fatal(err)
	}
	for i, _ := range rsl {
		_, err := nw.WriteString(rsl[i] + "\n")
		if err != nil {
			log.Fatal(err)
		}
		nr++
	}
	nw.Flush()
	fp.Close()
	log.Print("dosortvlrecfile test file ", fn)

	_, fns, err := dosortvlrecfile(fn, dn, "std", iomem)

	log.Println("dosortvlrecfile test after fns ", fns, " ", err)

	for _, f := range fns {
		log.Println("dosortvlrecfile chacking ", f)
		mfp, err := os.Open(f)
		if err != nil {
			log.Fatal(err)
		}
		lns, _, err = merge.Vlreadn(mfp, 0, iomem*2)
		//log.Println("dosortvlrecfile test lns ", len(lns))

		var slns = make([]string, 0)
		for _, l := range lns {
			slns = append(slns, string(l))
		}
		if slices.IsSorted(slns) == false {
			log.Fatal(f, " is not sorted")
		}
		nss += int64(len(lns))
	}
	if nrs != nss {
		log.Fatal("dosortvlrecfile test wanted ", nrs, " got ", nss)
	}
	log.Println("dosortvlrecfile passed")
}
