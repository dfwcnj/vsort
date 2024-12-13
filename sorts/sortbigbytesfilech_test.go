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

func Test_sortbigbytesfilech(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"

	var nr int

	for _, st := range stypes {
		for _, r := range bools {
			log.Printf("sortbigbytesfilech test %v %v", st, r)
			dn, err := initmergedir("/tmp", "sortbigbytesfilechtest")
			if err != nil {
				log.Fatal("sortbigbytesfilech test initmergedir ", err)
			}
			//log.Print("sortbigbytesfilech test initmergedir ", dn)

			rsl := randomdata.Randomstrings(nrs, rlen, r)

			fn := path.Join(dn, "sortbigbytesfilechtest")
			fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Fatalf("sortbigbytesfilech test %v open %v ", fn, err)
			}
			defer fp.Close()
			nw := bufio.NewWriter(fp)
			for _, s := range rsl {
				if r == true {
					_, err = nw.Write([]byte(s + "\n"))
				} else {
					_, err = nw.Write([]byte(s))
				}
				if err != nil {
					log.Fatal("sortbigbytesfilech test Write ", err)
				}
				nr++
			}
			nw.Flush()
			fp.Close()
			// log.Printf("sortbigbytesfilech test file %v %v %v", st, r, fn)

			var lns [][]byte
			var fns []string
			t0 := time.Now()
			if r == true {
				lns, fns, err = sortbigbytesfilech(fn, dn, st, 0, 0, 0, iomem)
			} else {
				lns, fns, err = sortbigbytesfilech(fn, dn, st, rlen, 0, rlen, iomem)
			}
			log.Printf("sortbigbytesfilech test %v %v duration %v", st, r, time.Since(t0))
			if len(lns) != 0 {
				log.Fatal("sortbigbytesfilech test lns ", len(lns))
			}

			var nss int64
			for _, f := range fns {
				// log.Printf("sortbigbytesfilech test fns %v", f)
				mfp, err := os.Open(f)
				if err != nil {
					log.Fatal("sortbigbytesfilech test open ", err)
				}
				finf, err := mfp.Stat()
				var slns []string
				if r == true {
					nss += filelinecount(f)
					slns, _, err = merge.Vlreadstrings(mfp, 0, finf.Size())
					if err != nil {
						log.Fatalf("sortbigbytesfilech test %v Vlreadstrings %v", f, err)
					}
				} else {
					nss += filereccount(f, rlen)
					slns, _, err = merge.Flreadstrings(mfp, 0, rlen, finf.Size())
					if err != nil {
						log.Fatalf("sortbigbytesfilech test %v Flreadstrings %v", f, err)
					}
				}

				if slices.IsSorted(slns) == false {
					t.Fatal("sortbigbytesfilech test failed  ", f, " is not sorted")
				}
			}
			if nrs != nss {
				t.Fatalf("sortbigbytesfilech test %v wanted %v got %v", dn, nrs, nss)
			}
		}
	}
	log.Print("sortbigbytesfilech test passed")
}
