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

func Test_csortbytesfilesch(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2)
	bools[0] = false
	bools[1] = true
	var stypes []string = make([]string, 4)
	stypes[0] = "heap"
	stypes[1] = "merge"
	stypes[2] = "radix"
	stypes[3] = "std"
	var nrs int64 = 1 << 20
	var iomem int64 = 1 << 20 * 8
	var nmf = 8

	for _, st := range stypes {
		for _, r := range bools {
			log.Printf("csortbytesfilesch test %v %v %v", nrs, st, r)

			dn, err := initmergedir("/tmp", "csortbytesfileschtest")
			if err != nil {
				log.Fatal("csortbytesfilesch test initmergedir ", err)
			}
			// log.Print("csortbytesfilesch test initmergedir ", dn)

			var fns []string
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
					t.Fatal("csortbytesfilesch test before sort wanted len ", nrs, " got ", len(lns))
				}

				var fn = filepath.Join(dn, fmt.Sprint("csortbytesfileschtest", i))
				// log.Printf("csortbytesfilesch test saving %v", fn)
				merge.Savebytemergefile(lns, fn)
				fns = append(fns, fn)
				if r == true {
					tns += filelinecount(fn)
				} else {
					tns += filereccount(fn, rlen)
				}
			}

			// log.Print("csortbytesfilesch test test files to sort ", fns)

			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)
			// log.Printf("csortbytesfilesch test mpath %v ", mpath)

			t0 := time.Now()
			if r == true {
				CSortbytesfilesch(fns, mpath, dn, st, 0, 0, 0, iomem)
			} else {
				CSortbytesfilesch(fns, mpath, dn, st, rlen, 0, rlen, iomem)
			}
			log.Printf("csortbytesfilesch test %v %v  duration %v", st, r, time.Since(t0))

			mfp, err := os.Open(mpath)
			if err != nil {
				t.Fatal("csortbytesfilesch test ", err)
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
						t.Fatal("csortbytesfilesch test  readfull ", err)
					}
					mlns = append(mlns, string(ln))
				}
			}
			if nlns != nrs*int64(nmf) {
				t.Fatal("csortbytesfilesch test ", nmf, " wanted ", int(nrs)*nmf, " got ", nlns)
			}
			if !slices.IsSorted(mlns) {
				t.Fatal("csortbytesfilesch test lines in ", mpath, " not in sort order")
			}
		}
	}
	log.Print("csortbytesfilesch test passed")

}
