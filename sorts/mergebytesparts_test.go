package sorts

import (
	"bufio"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_mergebytesparts(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	var nrs int64 = 1 << 20

	var nparts = 10
	var parts = make([][][]byte, 0, nparts)

	for _, r := range bools {
		log.Print("mergebytesparts test ", r)

		dn, err := initmergedir("/tmp", "mergebytefilestest")
		if err != nil {
			log.Fatal("mergebytefiles test initmergedir ", err)
		}

		for range nparts {
			var lns [][]byte

			rsl := randomdata.Randomstrings(nrs, rlen, r)

			for _, s := range rsl {
				ln := []byte(s)
				if r == true {
					ln = append(ln, "\n"...)
				}
				lns = append(lns, ln)
			}

			if r == true {
				rsort2ba(lns)
			} else {
				kvrsort2a(lns, rlen, 0, rlen)
			}

			parts = append(parts, lns)
		}
		// log.Printf("mergebytesparts test nparts %v ", len(parts))

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)
		// log.Printf("mergebytesparts test mpath %v ", mpath)

		if r == true {
			merge.Mergebytesparts(mpath, 0, 0, 0, parts)
		} else {
			merge.Mergebytesparts(mpath, rlen, 0, 0, parts)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("mergebytesparts test open ", err)
		}
		defer mfp.Close()

		//log.Print("counting merged records")
		var nlns int64
		var mlns []string
		if r == true {
			nlns = filelinecount(mpath)
			scanner := bufio.NewScanner(mfp)
			for scanner.Scan() {
				l := scanner.Text()
				mlns = append(mlns, l)
			}
		} else {
			nlns = filereccount(mpath, rlen)
			ln := make([]byte, rlen)
			for {
				_, err := io.ReadFull(mfp, ln)
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatal("mergebytesparts test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if nlns != nrs*int64(nparts) {
			t.Fatal("mergebytesparts test ", mpath, " wanted ", int(nrs)*nparts, " got ", nlns)
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergebytesparts test ", mpath, " not in sort order")
		}
	}
	log.Print("mergebytesparts test passed")

}
