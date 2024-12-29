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

func Test_sortvlbytesfile(t *testing.T) {
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
		log.Printf("sortvlbytesfile test %v %v", nrs, st)
		dn, err := initmergedir("/tmp", "sortvlbytesfiletest")
		if err != nil {
			log.Fatal("sortvlbytesfile test initmergedir ", err)
		}
		//log.Print("sortvlbytesfile test initmergedir ", dn)

		//log.Println("sortvlbytesfile test")

		rsl := randomdata.Randomstrings(nrs, rlen, r)

		fn := path.Join(dn, "sortvlbytesfiletest")
		fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("sortvlbytesfile test %v open %v", fn, err)
		}
		defer fp.Close()
		nw := bufio.NewWriter(fp)
		for i := range rsl {
			_, err := nw.WriteString(rsl[i] + "\n")
			if err != nil {
				log.Fatal("sortvlbytesfile test WriteString ", err)
			}
			nr++
		}
		nw.Flush()
		fp.Close()
		// log.Print("sortvlbytesfile test file ", fn)

		t0 := time.Now()
		lns, fns, err := sortvlbytesfile(fn, dn, st, iomem)
		if err != nil && err != io.EOF {
			t.Fatalf("sortvlbytesfile test sort %v %v %v", fn, st, err)
		}
		log.Printf("sortvlbytesfile test %v duration %v", st, time.Since(t0))
		if len(lns) != 0 {
			log.Fatal("sortvlbytesfile test lns ", len(lns))
		}

		//log.Println("sortvlbytesfile test after fns ", fns, " ", err)

		var nss int64
		for _, f := range fns {
			mfp, err := os.Open(f)
			if err != nil {
				log.Fatal("sortvlbytesfile test open ", err)
			}
			finf, _ := mfp.Stat()
			lns, _, err = merge.Vlreadbytes(mfp, 0, finf.Size())
			if err != nil && err != io.EOF {
				t.Fatalf("sortvlbytesfile test vlread %v %v %v", f, st, err)
			}
			//log.Println("sortvlbytesfile test lns ", len(lns))

			var slns = make([]string, 0)
			for _, l := range lns {
				slns = append(slns, string(l))
			}
			if slices.IsSorted(slns) == false {
				t.Fatal("sortvlbytesfile test failed  ", f, " is not sorted")
			}
			nss += int64(len(slns))
		}
		if nrs != nss {
			t.Fatal("sortvlbytesfile failed test wanted ", nrs, " got ", nss)
		}
	}
	log.Print("sortvlbytesfile test passed")
}
