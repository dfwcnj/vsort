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

func Test_sortstringsfiles(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var e bool = false
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)
	var nmf = 10

	for _, r := range bools {
		log.Print("Sortstringsfiles test ", r)

		dn, err := initmergedir("/tmp", "sortstringsfilestest")
		if err != nil {
			log.Fatal("Sortstringsfiles test initmergedir ", err)
		}
		//log.Print("Sortstringsfiles test initmergedir ", dn)

		var fns []string
		//log.Print("Sortstringsfiles test making ", nmf, " files to sort")
		var tns int64
		for i := range nmf {

			lns := randomdata.Randomstrings(nrs, rlen, r, e)
			if len(lns) != int(nrs) {
				log.Fatal("Sortstringsfiles test before sort wanted len ", nrs, " got ", len(lns))
			}

			var fn = filepath.Join(dn, fmt.Sprint("sortstringsfilestest", i))
			//log.Println("Sortstringsfiles test saving ", fn)
			merge.Savestringmergefile(lns, fn)
			fns = append(fns, fn)
			if r == true {
				tns += filelinecount(fn)
			} else {
				tns += filereccount(fn, rlen)
			}
		}

		// log.Print("Sortstringsfiles test test files record count ", tns)

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)

		if r == true {
			Sortstringsfiles(fns, mpath, "", "std", 0, 0, 0, iomem)
		} else {
			Sortstringsfiles(fns, mpath, "", "std", rlen, 0, 0, iomem)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("Sortstringsfiles test ", err)
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
					log.Fatal("Sortstringsfiles test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if len(mlns) != int(nrs)*nmf {
			t.Fatal("Sortstringsfiles test ", nmf, " wanted ", int(nrs)*nmf, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("Sortstringsfiles test lines in ", mpath, " not in sort order")
		}
	}
	log.Print("Sortstringsfiles test passed")

}
