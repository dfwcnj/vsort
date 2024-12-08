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

			rsort2ba(lns)

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
		var mlns []string
		if r == true {
			scanner := bufio.NewScanner(mfp)
			for scanner.Scan() {
				l := scanner.Text()
				mlns = append(mlns, l)
			}
		} else {
			for {
				ln := make([]byte, rlen)
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
		if len(mlns) != int(nrs)*nparts {
			t.Fatal("mergebytesparts test ", mpath, " wanted ", int(nrs)*nparts, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergebytesparts test ", mpath, " not in sort order")
		}
	}
	log.Print("mergebytesparts test passed")

}
