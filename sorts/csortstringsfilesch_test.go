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

func Test_csortstringsfilesch(t *testing.T) {
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
	var iomem int64 = nrs * int64(rlen/2)
	var nmf = 8

	for _, st := range stypes {
		for _, r := range bools {
			log.Printf("csortstringsfilesch test %v %v %v", nrs, st, r)

			dn, err := initmergedir("/tmp", "csortstringsfileschtest")
			if err != nil {
				log.Fatal("csortstringsfilesch test initmergedir ", err)
			}

			var fns []string
			var tns int64
			for i := range nmf {

				lns := randomdata.Randomstrings(nrs, rlen, r)
				if r == true {
					for i := range lns {
						lns[i] = lns[i] + "\n"
					}
				}

				var fn = filepath.Join(dn, fmt.Sprint("csortstringsfileschtest", i))
				merge.Savestringmergefile(lns, fn)
				fns = append(fns, fn)

				if r == true {
					tns += filelinecount(fn)
				} else {
					tns += filereccount(fn, rlen)
				}
			}

			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)

			t0 := time.Now()
			if r == true {
				CSortstringsfilesch(fns, mpath, "", st, 0, 0, 0, iomem)
			} else {
				CSortstringsfilesch(fns, mpath, "", st, rlen, 0, rlen, iomem)
			}
			log.Printf("csortstringsfilesch test %v %v duration %v", st, r, time.Since(t0))

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatalf("csortstringsfilesch test %v open %v ", mpath, err)
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
						log.Fatal("csortstringsfilesch test  readfull ", err)
					}
					mlns = append(mlns, string(ln))
				}
			}
			if len(mlns) != int(nrs)*nmf {
				t.Fatal("csortstringsfilesch test ", nmf, " wanted ", int(nrs)*nmf, " got ", len(mlns))
			}
			if !slices.IsSorted(mlns) {
				t.Fatal("csortstringsfilesch test lines in ", mpath, " not in sort order")
			}
		}
	}
	log.Print("csortstringsfilesch test passed")

}
