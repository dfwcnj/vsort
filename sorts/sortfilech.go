package sorts

import (
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/dfwcnj/vsort/merge"
)

// sortbytesfilech
// routine to split a file into pieces to sort concurrently
func sortbytesfilech(fn, ofn string, stype string, reclen, keyoff, keylen int, iomem int64) {
	// log.Print("sortbytesfilech")
	fp, err := os.Open(fn)
	if err != nil {
		log.Fatalf("sortbytesfilech open %v: %v", fn, err)
	}
	finf, err := fp.Stat()
	var fsz = finf.Size()

	// exceeds our iomem limit
	if fsz > iomem {
		log.Fatalf("sortbytesfilech file %v too large %v", fn, fsz)
		// sortbїgbytesfilech(fn, "", stype, reclen, keyoff, keylen, iomem)
	}

	lns, _, err := merge.Flreadbytes(fp, int64(0), reclen, iomem)
	if err != nil {
		log.Fatalf("sortbytesfilech read %v: %v", fn, err)
	}
	var nlns = len(lns)

	var nc = runtime.NumCPU()
	parts := splitbytesslice(lns, nc)

	inch := make(chan [][]byte, len(parts))

	var wg sync.WaitGroup
	wg.Add(len(parts))
	for _, part := range parts {
		go func() {
			defer wg.Done()
			sortbytesslicech(part, stype, reclen, keyoff, keylen, inch)
		}()
	}
	wg.Wait()

	tparts := make([][][]byte, 0, len(parts))
	var ns int
	for i := range tparts {
		tparts[i] = <-inch
		ns += len(tparts[i])
	}
	if ns != len(lns) {
		log.Fatalf("sortbytesfilech sortbytesslicech %v %v wanted %v got %v", fn, stype, nlns, ns)
	}

	merge.Mergebytesparts(ofn, reclen, keyoff, keylen, tparts)
}

// sortstringsfilech
// routine to split a file into pieces to sort concurrently
func sortstringsfilech(fn, ofn string, stype string, reclen, keyoff, keylen int, iomem int64) {
	// log.Print("sortstringsfilech")
	fp, err := os.Open(fn)
	if err != nil {
		log.Fatalf("sortstringsfilech open %v: %v", fn, err)
	}
	finf, err := fp.Stat()
	var fsz = finf.Size()

	if fsz > iomem {
		log.Fatalf("sortstringsfilech %v too large %v", fn, fsz)
		// sortbїgstringsfilech(fn, "", stype, reclen, keyoff, keylen, iomem)
	}

	lns, _, err := merge.Flreadstrings(fp, int64(0), reclen, iomem)
	if err != nil {
		log.Fatalf("sortstringsfilech read %v: %v", fn, err)
	}
	var nlns = len(lns)

	var nc = runtime.NumCPU()
	parts := splitstringsslice(lns, nc)

	inch := make(chan []string, len(parts))

	var wg sync.WaitGroup
	wg.Add(len(parts))
	for _, part := range parts {
		go func() {
			defer wg.Done()
			sortstringsslicech(part, stype, reclen, keyoff, keylen, inch)
		}()
	}
	wg.Wait()

	tparts := make([][]string, 0, len(parts))
	var ns int
	for i := range tparts {
		tparts[i] = <-inch
		ns += len(tparts[i])
	}
	if ns != len(lns) {
		log.Fatalf("sortstringsfilech sortstringsslicech %v %v wanted %v got %v", fn, stype, nlns, ns)
	}

	merge.Mergestringsparts(ofn, reclen, keyoff, keylen, tparts)
}
