package sorts

import (
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_sortbytesslicech(t *testing.T) {
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
	var ns int = 10

	for _, st := range stypes {
		for _, r := range bools {
			log.Printf("sortbytesslicech test %v %v", st, r)

			rsl := randomdata.Randomstrings(nrs, rlen, r)
			lns := make([][]byte, 0, nrs)
			for _, s := range rsl {
				ln := []byte(s)
				if r == true {
					ln = append(ln, "\n"...)
				}
				lns = append(lns, ln)
			}

			parts := splitbytesslice(lns, ns)
			var np = len(parts)
			log.Printf("sortbytesslicech split into %v parts", np)

			inch := make(chan [][]byte, np)
			var wg sync.WaitGroup
			wg.Add(len(parts))

			t0 := time.Now()
			for i := range np {
				if r == true {
					go func() {
						defer wg.Done()
						sortbytesslicech(parts[i], st, 0, 0, 0, inch)
					}()
				} else {
					go func() {
						defer wg.Done()
						sortbytesslicech(parts[i], st, rlen, 0, rlen, inch)
					}()
				}
			}
			wg.Wait()
			t1 := time.Now()

			log.Printf("sortbytesslicech test sort %v duration %v", st, t1.Sub(t0))

			for i := range np {
				ss := make([]string, 0, len(parts[i]))
				for _, ln := range parts[i] {
					ss = append(ss, string(ln))
				}
				if !slices.IsSorted(ss) {
					t.Fatalf("sortbytesslicech %v part %v not sorted", st, i)
				}
			}
		}
	}
	log.Print("sortbytesslicech test passed")

}
