package sorts

import (
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dfwcnj/randomdata"
)

func Test_sortstringsslicech(t *testing.T) {
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
			log.Printf("sortstringsslicech test %v %v", st, r)

			rsl := randomdata.Randomstrings(nrs, rlen, r)

			parts := splitstringsslice(rsl, ns)
			var np = len(parts)
			// log.Printf("sortstringsslicech split into %v parts", np)

			inch := make(chan []string, np)
			var wg sync.WaitGroup
			wg.Add(len(parts))

			t0 := time.Now()
			for i := range np {
				if r == true {
					go func() {
						defer wg.Done()
						sortstringsslicech(parts[i], st, 0, 0, 0, inch)
					}()
				} else {
					go func() {
						defer wg.Done()
						sortstringsslicech(parts[i], st, rlen, 0, rlen, inch)
					}()
				}
			}
			wg.Wait()
			log.Printf("sortstringsslicech test sort %v %v duration %v", st, r, time.Since(t0))

			for i := range np {
				if !slices.IsSorted(parts[i]) {
					t.Fatalf("sortstringsslicech %v part %v not sorted", st, i)
				}
			}
		}
	}
	log.Print("sortstringsslicech test passed")

}
