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
	var nrs int64 = 1 << 20
	var iomem int64 = 1 << 28
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"

	var nr int

	for _, st := range stypes {
		for _, r := range bools {
			log.Print("sortbytesfilech test ", st, " ", r)
			dn, err := initmergedir("/tmp", "sortbytesfilechtest")
			if err != nil {
				log.Fatal("sortbytesfilech test initmergedir ", err)
			}
			//log.Print("sortbytesfilech test initmergedir ", dn)

			log.Println("sortbytesfilech test")

			rsl := randomdata.Randomstrings(nrs, rlen, r)

			fn := path.Join(dn, "sortbytesfilechtest")
			fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			defer fp.Close()
			nw := bufio.NewWriter(fp)
			if err != nil {
				log.Fatal("sortbytesfilech test NewWriter ", err)
			}
			for i := range rsl {
				if r == true {
					_, err = nw.Write([]byte(rsl[i] + "\n"))
				} else {
					_, err = nw.Write([]byte(rsl[i]))
				}
				if err != nil {
					log.Fatal("sortbytesfilech test Write ", err)
				}
				nr++
			}
			nw.Flush()
			fp.Close()
			// make output file string
			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)
			//log.Print("merge.Mergebytefiles ", fns)
			//log.Print("sortbytesfilech test file ", fn)

			t0 := time.Now()
			sortbytesfilech(fn, mpath, st, rlen, 0, rlen, iomem)
			log.Printf("sortbytesfilech test %v %v duration %v", st, r, time.Since(t0))

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatalf("sortbytesfilech test %vopen %v", mpath, err)
			}
			finf, err := mfp.Stat()

			var slns = make([]string, 0)
			if r == true {
				slns, _, err = merge.Vlreadstrings(mfp, 0, finf.Size())
			} else {
				slns, _, err = merge.Flreadstrings(mfp, 0, rlen, finf.Size())
			}
			//log.Println("sortbytesfilech test lns ", len(lns))

			if slices.IsSorted(slns) == false {
				t.Fatal("sortbytesfilech test failed  ", mpath, " is not sorted")
			}
			if nrs != int64(len(slns)) {
				t.Fatal("sortbytesfilech failed test wanted ", nrs, " got ", len(slns))
			}
		}
	}
	log.Print("sortbytesfilech test passed")
}
