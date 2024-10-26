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
	"time"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_sortbytesfiles(t *testing.T) {
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
			log.Print("sortbytesfiles test ", st, " ", r)

			dn, err := initmergedir("/tmp", "sortbytesfilestest")
			if err != nil {
				log.Fatal("sortbytesfiles test initmergedir ", err)
			}
			//log.Print("sortbytesfiles test initmergedir ", dn)

			var fns []string
			//log.Print("sortbytesfiles test making ", nmf, " files to sort")
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
					log.Fatal("sortbytesfiles test before sort wanted len ", nrs, " got ", len(lns))
				}

				var fn = filepath.Join(dn, fmt.Sprint("sortbytesfilestest", i))
				//log.Println("sortbytesfiles test saving ", fn)
				merge.Savebytemergefile(lns, fn)
				fns = append(fns, fn)
				if r == true {
					tns += filelinecount(fn)
				} else {
					tns += filereccount(fn, rlen)
				}
			}

			// log.Print("sortbytesfiles test test files record count ", tns)

			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)

			t0 := time.Now()
			if r == true {
				Sortbytesfiles(fns, mpath, "", st, 0, 0, 0, iomem)
			} else {
				Sortbytesfiles(fns, mpath, "", st, rlen, 0, rlen, iomem)
			}
			t1 := time.Now()
			log.Printf("sortbytesfiles test sort duration %v", t1.Sub(t0))

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatal("sortbytesfiles test ", err)
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
						log.Fatal("sortbytesfiles test  readfull ", err)
					}
					mlns = append(mlns, string(ln))
				}
			}
			if nlns != nrs*int64(nmf) {
				t.Fatal("sortbytesfiles test ", nmf, " wanted ", int(nrs)*nmf, " got ", nlns)
			}
			if !slices.IsSorted(mlns) {
				t.Fatal("sortbytesfiles test lines in ", mpath, " not in sort order")
			}
		}
	}
	log.Print("sortbytesfiles test passed")

}
