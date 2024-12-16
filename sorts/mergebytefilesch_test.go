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

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_mergebytefilesch(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	var nrs int64 = 1 << 20

	var nmf = 10

	for _, r := range bools {
		log.Print("mergebytefilesch test ", r)
		var fns []string
		dn, err := initmergedir("/tmp", "mergebytefilestestch")
		if err != nil {
			log.Fatal("mergebytefilesch test initmergedir ", err)
		}
		// log.Print("mergebytefilesch test initmergedir ", dn)

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

			if r == true {
				rsort2ba(lns)
			} else {
				kvrsort2a(lns, rlen, 0, rlen)
			}

			var fn = filepath.Join(dn, fmt.Sprint("file", i))
			merge.Savebytemergefile(lns, fn)
			fns = append(fns, fn)
		}

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)
		//log.Print("merge.Mergebytefiles ", fns)

		if r == true {
			merge.Mergebytefilesch(mpath, 0, 0, 0, fns)
		} else {
			merge.Mergebytefilesch(mpath, rlen, 0, rlen, fns)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("mergebytefilesch test open ", err)
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
					log.Fatal("mergebytefilesch test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if len(mlns) != int(nrs)*nmf {
			t.Fatal("mergebytefilesch test ", mpath, " wanted ", int(nrs)*nmf, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergebytefilesch test ", mpath, " not in sort order")
		}
	}
	log.Print("mergebytefilesch test passed")

}
