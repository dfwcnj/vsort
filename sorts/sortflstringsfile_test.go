package sorts

import (
	"bufio"
	"log"
	"os"
	"path"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_sortflstringsfile(t *testing.T) {
	var rlen int = 32
	var r bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)
	var stypes []string = make([]string, 4, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"

	var nr int

	for _, st := range stypes {
		log.Print("sortflstringsfile test ", st)
		dn, err := initmergedir("/tmp", "sortflstringsfiletest")
		if err != nil {
			log.Fatal("sortflstringsfile test initmergedir ", err)
		}
		//log.Print("sortflstringsfile test initmergedir ", dn)

		//log.Println("sortflstringsfile test")

		rsl := randomdata.Randomstrings(nrs, rlen, r)

		fn := path.Join(dn, "sortflstringsfiletest")
		fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		defer fp.Close()
		nw := bufio.NewWriter(fp)
		if err != nil {
			log.Fatal("sortflstringsfile test NewWriter ", err)
		}
		for i, _ := range rsl {
			_, err := nw.WriteString(rsl[i] + "\n")
			if err != nil {
				log.Fatal("sortflstringsfile test WriteString ", err)
			}
			nr++
		}
		nw.Flush()
		fp.Close()
		//log.Print("sortflstringsfile test file ", fn)

		lns, fns, err := sortflstringsfile(fn, dn, "std", rlen+1, 0, rlen, iomem)
		if len(lns) != 0 {
			log.Fatal("sortflstringsfile test lns ", len(lns))
		}

		//log.Println("sortflstringsfile test after fns ", fns, " ", err)

		var nss int64
		for _, f := range fns {
			mfp, err := os.Open(f)
			if err != nil {
				log.Fatal("sortflstringsfile test open ", err)
			}
			finf, err := mfp.Stat()
			lns, _, err = merge.Vlreadstrings(mfp, 0, finf.Size())

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
