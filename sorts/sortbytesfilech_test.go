package sorts

import (
	"bufio"
	"log"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_sortbytesfilech(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	var nrs int64 = 1 << 23
	var iomem int64 = 1 << 28
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"

	var nr int

	for _, st := range stypes {
		for _, r := range bools {
			dn, err := initmergedir("/tmp", "sortbytesfilechtest")
			if err != nil {
				log.Fatal("sortbytesfilech test initmergedir ", err)
			}

			lns := randomdata.Randomstrings(nrs, rlen, r)

			fn := path.Join(dn, "sortbytesfilechtest")
			fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Fatalf("sortbytesfilech %v open %v ", fn, err)
			}
			defer fp.Close()
			nw := bufio.NewWriter(fp)
			for i := range lns {
				if r == true {
					_, err = nw.Write([]byte(lns[i] + "\n"))
				} else {
					_, err = nw.Write([]byte(lns[i]))
				}
				if err != nil {
					log.Fatalf("sortbytesfilech test %v Write %v", fn, err)
				}
				nr++
			}
			nw.Flush()
			fp.Close()

			log.Printf("sortbytesfilech test %v %v %v", nrs, st, r)

			// make output file string
			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)

			t0 := time.Now()
			if r == true {
				sortbytesfilech(fn, mpath, st, 0, 0, 0, iomem)
			} else {
				sortbytesfilech(fn, mpath, st, rlen, 0, rlen, iomem)
			}
			log.Printf("sortbytesfilech test %v %v duration %v", st, r, time.Since(t0))

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatalf("sortbytesfilech test %vopen %v", mpath, err)
			}
			// finf, err := mfp.Stat()

			var slns []string
			if r == true {
				slns, _, err = merge.Vlreadstrings(mfp, 0, iomem)
			} else {
				slns, _, err = merge.Flreadstrings(mfp, 0, rlen, iomem)
			}
			if err != nil {
				t.Fatalf("sortbytesfilech test readstrings %v %v %v", mpath, len(slns), err)
			}
			if nrs != int64(len(slns)) {
				t.Fatalf("sortbytesfilech test %v wanted %v got %v", mpath, nrs, len(slns))
			}

			if slices.IsSorted(slns) == false {
				t.Fatal("sortbytesfilech test failed  ", mpath, " is not sorted")
			}
		}
	}
	log.Print("sortbytesfilech test passed")
}
