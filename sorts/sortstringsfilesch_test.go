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

func Test_chsortstringsfiles(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var stypes []string = make([]string, 4, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"
	var nrs int64 = 1 << 20
	var iomem int64 = nrs * int64(rlen/2)
	var nmf = 10

	for _, st := range stypes {
		for _, r := range bools {
			log.Print("chSortstringsfiles test ", st, " ", r)

			dn, err := initmergedir("/tmp", "chsortstringsfilestest")
			if err != nil {
				log.Fatal("chSortstringsfiles test initmergedir ", err)
			}
			//log.Print("chSortstringsfiles test initmergedir ", dn)

			var fns []string
			//log.Print("chSortstringsfiles test making ", nmf, " files to sort")
			var tns int64
			for i := range nmf {

				lns := randomdata.Randomstrings(nrs, rlen, r)
				if len(lns) != int(nrs) {
					log.Fatal("chSortstringsfiles test before sort wanted len ", nrs, " got ", len(lns))
				}
				if r == true {
					for i, _ := range lns {
						lns[i] = lns[i] + "\n"
					}
				}

				var fn = filepath.Join(dn, fmt.Sprint("chsortstringsfilestest", i))
				//log.Println("chSortstringsfiles test saving ", fn)
				merge.Savestringmergefile(lns, fn)
				fns = append(fns, fn)
				if r == true {
					tns += filelinecount(fn)
				} else {
					tns += filereccount(fn, rlen)
				}
			}

			// log.Print("chSortstringsfiles test test files record count ", tns)

			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)

			if r == true {
				chSortstringsfiles(fns, mpath, "", st, 0, 0, 0, iomem)
			} else {
				chSortstringsfiles(fns, mpath, "", st, rlen, 0, rlen, iomem)
			}

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatal("chSortstringsfiles test ", err)
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
						log.Fatal("chSortstringsfiles test  readfull ", err)
					}
					mlns = append(mlns, string(ln))
				}
			}
			if len(mlns) != int(nrs)*nmf {
				t.Fatal("chSortstringsfiles test ", nmf, " wanted ", int(nrs)*nmf, " got ", len(mlns))
			}
			if !slices.IsSorted(mlns) {
				t.Fatal("chSortstringsfiles test lines in ", mpath, " not in sort order")
			}
		}
	}
	log.Print("chSortstringsfiles test passed")

}
