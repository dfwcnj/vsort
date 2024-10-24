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

func Test_sortbytesfilesch(t *testing.T) {
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
			log.Print("sortbytesfilesch test ", st, " ", r)

			dn, err := initmergedir("/tmp", "sortbytesfileschtest")
			if err != nil {
				log.Fatal("sortbytesfilesch test initmergedir ", err)
			}
			//log.Print("sortbytesfilesch test initmergedir ", dn)

			var fns []string
			//log.Print("sortbytesfilesch test making ", nmf, " files to sort")
			var tns int64
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
				if len(lns) != int(nrs) {
					log.Fatal("sortbytesfilesch test before sort wanted len ", nrs, " got ", len(lns))
				}

				var fn = filepath.Join(dn,
					fmt.Sprint("sortbytesfilesch", i))
				//log.Println("sortbytesfilesch test saving ", fn)
				merge.Savebytemergefile(lns, fn)
				fns = append(fns, fn)
				if r == true {
					tns += filelinecount(fn)
				} else {
					tns += filereccount(fn, rlen)
				}
			}

			// log.Print("sortbytesfilesch test test files record count ", tns)

			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)

			if r == true {
				Sortbytesfilesch(fns, mpath, "", st, 0, 0, 0, iomem)
			} else {
				Sortbytesfilesch(fns, mpath, "", st, rlen, 0, rlen, iomem)
			}

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatal("sortbytesfilesch test ", err)
			}
			defer mfp.Close()

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
						log.Fatal("sortbytesfilesch test  readfull ", err)
					}
					mlns = append(mlns, string(ln))
				}
			}
			if nlns != nrs*int64(nmf) {
				t.Fatal("sortbytesfilesch test ", nmf, " wanted ", int(nrs)*nmf, " got ", nlns)
			}
			if !slices.IsSorted(mlns) {
				t.Fatal("sortbytesfilesch test lines in ", mpath, " not in sort order")
			}
		}
	}
	log.Print("sortbytesfilesch test passed")

}
