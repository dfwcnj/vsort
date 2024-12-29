package sorts

import (
	"bufio"
	"io"
	"log"
	"os"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_sortflstringsfile(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"

	var nr int

	for _, st := range stypes {
		log.Printf("sortflstringsfile test %v %v", nrs, st)
		dn, err := initmergedir("/tmp", "sortflstringsfiletest")
		if err != nil {
			log.Fatal("sortflstringsfile test initmergedir ", err)
		}

		rsl := randomdata.Randomstrings(nrs, rlen, r)

		fn := path.Join(dn, "sortflstringsfiletest")
		fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("sortflstringsfile test %v open %v ", fn, err)
		}
		defer fp.Close()
		nw := bufio.NewWriter(fp)
		for i := range rsl {
			_, err := nw.WriteString(rsl[i])
			if err != nil {
				log.Fatal("sortflstringsfile test WriteString ", err)
			}
			nr++
		}
		nw.Flush()
		fp.Close()

		t0 := time.Now()
		lns, fns, err := sortflstringsfile(fn, dn, st, rlen, 0, rlen, iomem)
		if err != nil && err != io.EOF {
			t.Fatalf("sortflstringsfile test sort %v %v %v", fn, st, err)
		}
		log.Printf("sortflstringsfile %v duration %v", st, time.Since(t0))
		if len(lns) != 0 {
			log.Fatal("sortflstringsfile test lns ", len(lns))
		}

		var nss int64
		for _, f := range fns {
			mfp, err := os.Open(f)
			if err != nil {
				log.Fatal("sortflstringsfile test open ", err)
			}
			finf, _ := mfp.Stat()
			lns, _, err = merge.Flreadstrings(mfp, 0, rlen, finf.Size())
			if err != nil && err != io.EOF {
				t.Fatalf("sortflstringsfile test flread %v %v", f, err)
			}

			var slns = make([]string, 0)
			for _, l := range lns {
				slns = append(slns, string(l))
			}
			if slices.IsSorted(slns) == false {
				t.Fatal("sortflstringsfile test failed  ", f, " is not sorted")
			}
			nss += int64(len(slns))
		}
		if nrs != nss {
			t.Fatal("sortflstringsfile failed test wanted ", nrs, " got ", nss)
		}
	}
	log.Print("sortflstringsfile test passed")
}
