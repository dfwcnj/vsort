package sorts

import (
	"bufio"
	"log"
	"os"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_sortvlstringsfile(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)

	var nr int

	for _, st := range stypes {
		log.Printf("sortvlstringsfile test %v %v", nrs, st)
		dn, err := initmergedir("/tmp", "sortvlstringsfiletest")
		if err != nil {
			log.Fatal("sortvlstringsfile test initmergedir ", err)
		}

		ulns := randomdata.Randomstrings(nrs, rlen, r)

		fn := path.Join(dn, "sortvlstringsfiletest")
		fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		defer fp.Close()
		nw := bufio.NewWriter(fp)
		if err != nil {
			log.Fatal("sortvlstringsfile test NewWriter ", err)
		}
		for i := range ulns {
			_, err := nw.WriteString(ulns[i] + "\n")
			if err != nil {
				log.Fatal("sortvlstringsfile test WriteString ", err)
			}
			nr++
		}
		nw.Flush()
		fp.Close()

		t0 := time.Now()
		lns, fns, err := sortvlstringsfile(fn, dn, st, iomem)
		log.Printf("sortvlstringsfile %v duration %v", st, time.Since(t0))
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
			lns, _, err = merge.Vlreadstrings(mfp, 0, finf.Size())

			if slices.IsSorted(lns) == false {
				t.Fatal("sortvlstringsfile test failed  ", f, " is not sorted")
			}
			nss += int64(len(lns))
		}
		if nrs != nss {
			t.Fatal("sortvlstringsfile failed test wanted ", nrs, " got ", nss)
		}
	}
	log.Print("sortvlstringsfile test passed")
}
