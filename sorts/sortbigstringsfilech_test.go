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

func Test_sortbigstringsfilech(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * 8
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"

	var nr int

	for _, st := range stypes {
		for _, r := range bools {
			log.Printf("sortbigstringsfilech test %v %v", st, r)

			dn, err := initmergedir("/tmp", "sortbigstringsfilechtest")
			if err != nil {
				log.Fatal("sortbigstringsfilech test initmergedir ", err)
			}
			//log.Print("sortbigstringsfilech test initmergedir ", dn)

			rsl := randomdata.Randomstrings(nrs, rlen, r)

			fn := path.Join(dn, "sortbigstringsfilechtest")
			fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Fatalf("sortbigstringsfilech test %v open %v ", fn, err)
			}
			defer fp.Close()

			nw := bufio.NewWriter(fp)
			for i := range rsl {
				if r == true {
					_, err = nw.WriteString(rsl[i] + "\n")
				} else {
					_, err = nw.WriteString(rsl[i])
				}
				if err != nil {
					log.Fatal("sortbigstringsfilech test WriteString ", err)
				}
				nr++
			}
			nw.Flush()
			finf, err := fp.Stat()
			fp.Close()
			// log.Printf("sortbigstringsfilech test %v size %v ", fn, finf.Size())

			var lns []string
			var fns []string
			t0 := time.Now()
			if r == true {
				lns, fns, err = sortbigstringsfilech(fn, dn, st, 0, 0, 0, iomem)
			} else {
				lns, fns, err = sortbigstringsfilech(fn, dn, st, rlen+1, 0, rlen, iomem)
			}
			log.Printf("sortbigstringsfilech test %v %v duration %v", st, r, time.Since(t0))
			if len(lns) != 0 {
				log.Fatal("sortbigstringsfilech test lns ", len(lns))
			}

			var nss int64
			for _, f := range fns {
				mfp, err := os.Open(f)
				if err != nil {
					log.Fatal("sortbigstringsfilech test open ", err)
				}

				// finf, err := mfp.Stat()
				// log.Printf("sortbigstringsfilech test after sort %v %v %v", fn, r, finf.Size())
				if r == true {
					lns, _, err = merge.Vlreadstrings(mfp, 0, finf.Size()*2)
				} else {
					lns, _, err = merge.Flreadstrings(mfp, 0, rlen, finf.Size()*2)
				}
				log.Printf("sortbigstringsfilech test readstrings %v %v lns", f, len(lns))

				if slices.IsSorted(lns) == false {
					t.Fatal("sortbigstringsfilech test failed  ", f, " is not sorted")
				}
				nss += int64(len(lns))
			}
			if nrs != nss {
				t.Fatal("sortbigstringsfilech test failed test wanted ", nrs, " got ", nss)
			}
		}
	}
	log.Print("sortbigstringsfilech test passed")
}
