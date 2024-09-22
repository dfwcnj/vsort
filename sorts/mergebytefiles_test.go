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

func Test_mergebytefiles(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20

	var nmf = 10

	for _, r := range bools {
		log.Print("mergebytefiles test ", r)
		var fns []string
		dn, err := initmergedir("/tmp", "mergebytefilestest")
		if err != nil {
			log.Fatal("mergebytefiles test initmergedir ", err)
		}
		//log.Print("mergebytefiles test initmergedir ", dn)

		for i := range nmf {
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

			var fn = filepath.Join(dn, fmt.Sprint("file", i))
			merge.Savebytemergefile(lns, fn)
			fns = append(fns, fn)
		}

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)
		//log.Print("merge.Mergebytefiles ", fns)

		if r == true {
			merge.Mergebytefiles(mpath, 0, 0, 0, fns)
		} else {
			merge.Mergebytefiles(mpath, rlen, 0, 0, fns)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("mergebytefiles test open ", err)
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
					log.Fatal("mergebytefiles test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if len(mlns) != int(nrs)*nmf {
			t.Fatal("mergebytefiles test ", mpath, " wanted ", int(nrs)*nmf, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergebytefiles test ", mpath, " not in sort order")
		}
	}
	log.Print("mergebytefiles test passed")

}
