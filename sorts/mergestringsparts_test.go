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

func Test_mergestringsparts(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20

	var nparts = 10
	var parts = make([][]string, 0, nparts)

	for _, r := range bools {

		log.Printf("mergestringsparts test %v %v", rlen, r)

		dn, err := initmergedir("/tmp", "mergestringsparts")
		if err != nil {
			log.Fatal("mergestringsparts test initmergedir ", err)
		}
		//log.Print("mergestringsparts test initmergedir ", dn)

		for range nparts {

			lns := randomdata.Randomstrings(nrs, rlen, r)
			// random length strings must be newline delimited
			if r == true {
				for i := range lns {
					lns[i] = lns[i] + "\n"
				}
			}

			if r == true {
				rsort2sa(lns, 0, 0, 0)
			} else {
				rsort2sa(lns, rlen, 0, rlen)
			}

			parts = append(parts, lns)
		}

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)
		//log.Print("merge.mergestringsparts ", fns)

		if r == true {
			merge.Mergestringsparts(mpath, 0, 0, 0, parts)
		} else {
			merge.Mergestringsparts(mpath, rlen, 0, rlen, parts)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("mergestringsparts test open ", err)
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
			for {
				ln := make([]byte, rlen)
				_, err := io.ReadFull(mfp, ln)
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatal("mergestringsparts test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if nlns != nrs*int64(nparts) {
			t.Fatal("mergestringsparts test ", mpath, " wanted ", int(nrs)*nparts, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergestringsparts test ", mpath, " not in sort order")
		}
	}
	log.Print("mergestringsparts test passed")

}
