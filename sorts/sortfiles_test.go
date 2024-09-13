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

func Test_sortfiles(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)
	var nmf = 10

	for _, r := range bools {
		log.Print("mergefiles test", r)
		log.Print("sortfiles test")

		dn, err := initmergedir("/tmp", "sortfilestest")
		if err != nil {
			log.Fatal("sortfile test initmergedir ", err)
		}

		var fns []string
		log.Print("sortfiles test making ", nmf, " files to sort")
		var tns int64
		for i := range nmf {
			var lns [][]byte

			rsl := randomdata.Randomstrings(nrs, rlen, r, e)
			for _, s := range rsl {
				ln := []byte(s)
				if r == true {
					ln = append(ln, "\n"...)
				}
				lns = append(lns, ln)
			}
			if len(lns) != int(nrs) {
				log.Fatal("sortfiles test before sort wanted len ", nrs, " got ", len(lns))
			}

			var fn = filepath.Join(dn, fmt.Sprint("sortfilestest", i))
			// log.Println("sortfiles test saving ", fn)
			merge.Savemergefile(lns, fn)
			fns = append(fns, fn)
			tns += filelinecount(fn)
		}
		log.Print("sortfiles test test files line count ", tns)

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)

		Sortfiles(fns, mpath, "", "std", 0, 0, 0, iomem)

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("sortfiles test ", err)
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
			t.Fatal("sortfiles test ", nmf, " wanted ", int(nrs)*nmf, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("sortfiles test lines in ", mfn, " not in sort order")
		}
	}
	log.Print("sortfiles test passed")

}
