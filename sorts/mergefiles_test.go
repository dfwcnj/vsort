package sorts

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
)

func Test_mergefiles(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 0, 2)
	bools[0] = true
	bools[1] = false
	var e bool = false
	var nrs int64 = 1 << 20

	var nmf = 10
	var fns []string

	for _, r := range bools {
		log.Print("mergefiles test", r)
		dn, err := initmergedir("/tmp", "mergefilestest")
		if err != nil {
			log.Fatal("mergefiles test initmergedir ", err)
		}

		for i := range nmf {
			var lns [][]byte

			rsl := randomdata.Randomstrings(nrs, rlen, r, e)

			for _, s := range rsl {
				ln := []byte(s)
				lns = append(lns, ln)
			}

			rsort2a(lns)

			var fn = filepath.Join(dn, fmt.Sprint("file", i))
			if r == true {
				merge.Savemergefile(lns, fn, "\n")
			} else {
				merge.Savemergefile(lns, fn, "")
			}
			fns = append(fns, fn)
		}

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)
		//log.Print("merge.Mergefiles ", fns)

		if r == true {
			merge.Mergefiles(mpath, 0, 0, 0, fns)
		} else {
			merge.Mergefiles(mpath, rlen, 0, 0, fns)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("mergefiles test open ", err)
		}
		defer mfp.Close()

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
					log.Fatal("mergefiles test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if len(mlns) != int(nrs)*nmf {
			t.Fatal("mergefiles test ", mpath, " wanted ", int(nrs)*nmf, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergefiles test ", mpath, " not in sort order")
		}
	}
	log.Print("mergefiles test passed")

}
