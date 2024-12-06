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

func Test_sortstringsfilesch(t *testing.T) {
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
	var nmf = 10

	for _, st := range stypes {
		for _, r := range bools {
			log.Print("sortstringsfilesch test ", st, " ", r)

			dn, err := initmergedir("/tmp", "sortstringsfileschtest")
			if err != nil {
				log.Fatal("sortstringsfilesch test initmergedir ", err)
			}
			//log.Print("sortstringsfilesch test initmergedir ", dn)

			var fns []string
			//log.Print("sortstringsfilesch test making ", nmf, " files to sort")
			var tns int64
			for i := range nmf {

				lns := randomdata.Randomstrings(nrs, rlen, r)
				if len(lns) != int(nrs) {
					log.Fatal("sortstringsfilesch test before sort wanted len ", nrs, " got ", len(lns))
				}
				if r == true {
					for i, _ := range lns {
						lns[i] = lns[i] + "\n"
					}
				}

				var fn = filepath.Join(dn, fmt.Sprint("sortstringsfileschtest", i))
				//log.Println("sortstringsfilesch test saving ", fn)
				merge.Savestringmergefile(lns, fn)
				fns = append(fns, fn)
				if r == true {
					tns += filelinecount(fn)
				} else {
					tns += filereccount(fn, rlen)
				}
			}

			// log.Print("sortstringsfilesch test test files record count ", tns)

			mfn := "mergeout.txt"
			mpath := filepath.Join(dn, mfn)

			t0 := time.Now()
			if r == true {
				Sortstringsfilesch(fns, mpath, "", st, 0, 0, 0, iomem)
			} else {
				Sortstringsfilesch(fns, mpath, "", st, rlen, 0, rlen, iomem)
			}
			t1 := time.Now()
			log.Printf("sortstringsfilesch test sort duration %v", t1.Sub(t0))

			mfp, err := os.Open(mpath)
			if err != nil {
				log.Fatal("sortstringsfilesch test ", err)
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
						log.Fatal("sortstringsfilesch test  readfull ", err)
					}
					mlns = append(mlns, string(ln))
				}
			}
			if len(mlns) != int(nrs)*nmf {
				t.Fatal("sortstringsfilesch test ", nmf, " wanted ", int(nrs)*nmf, " got ", len(mlns))
			}
			if !slices.IsSorted(mlns) {
				t.Fatal("sortstringsfilesch test lines in ", mpath, " not in sort order")
			}
		}
	}
	log.Print("sortstringsfilesch test passed")

}
