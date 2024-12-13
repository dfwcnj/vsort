package sorts

import (
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/dfwcnj/vsort/merge"
)

func splitbytesslice(lns [][]byte, ns int) [][][]byte {
	var parts [][][]byte
	var pl int = len(lns) / ns

	var off int
	for {
		if off >= len(lns) {
			break
		}
		if len(lns)-(off+pl) < pl/2 {
			parts = append(parts, lns[off:])
			break
		}
		parts = append(parts, lns[off:off+pl])
		off += pl
	}
	return parts
}

func splitstringsslice(lns []string, ns int) [][]string {
	var parts [][]string
	var pl int = len(lns) / ns

	var off int
	for {
		if off >= len(lns) {
			break
		}
		if len(lns)-(off+pl) < pl/2 {
			parts = append(parts, lns[off:])
			break
		}
		parts = append(parts, lns[off:off+pl])
		off += pl
	}
	return parts
}

// sortbytesslicech
func sortbytesslicech(lns [][]byte, stype string, reclen, keyoff, keylen int, ouch chan [][]byte) {
	// log.Printf("sortbytesslicech %v %v lines", stype, len(lns))
	switch stype {
	case "heap":
		kvbheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvbinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		kvbmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		if keylen > 0 {
			kvrsort2a(lns, reclen, keyoff, keylen)
		} else {
			rsort2ba(lns)
		}
	case "std":
		kvslicesbsort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortbytesslicech stype ", stype)
	}
	ouch <- lns
}

// sortstringsslicech
func sortstringsslicech(lns []string, stype string, reclen, keyoff, keylen int, ouch chan []string) {
	// log.Printf("sortstringsslicech %v %v lines", stype, len(lns))
	switch stype {
	case "heap":
		kvsheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvsinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		kvsmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		rsort2sa(lns, reclen, keyoff, keylen)
	case "std":
		kvslicesssort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortstringsslicech stype ", stype)
	}
	ouch <- lns
}

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

	var nc = runtime.NumCPU()
	// readthe bytes
	lns, _, err := merge.Flreadbytes(fp, int64(0), reclen, fsz)
	if err != nil {
		log.Fatalf("sortbytesfilech read %v: %v", fn, err)
	}

	parts := splitbytesslice(lns, nc)
	// create a byte slice channel with n parts capacity
	inch := make(chan [][]byte, len(parts))
	var wg sync.WaitGroup
	wg.Add(len(parts))

	for i := range parts {
		go func() {
			defer wg.Done()
			// log.Printf("sortbytesfilech %v", i)
			sortbytesslicech(parts[i], stype, reclen, keyoff, keylen, inch)
		}()
	}

	wg.Wait()
	tparts := make([][][]byte, len(parts))

	for i := range tparts {
		tparts[i] = <-inch
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

	// exceeds our iomem limits
	if fsz > iomem {
		log.Fatalf("sortstringsfilech %v too large %v", fn, fsz)
		// sortbїgstringsfilech(fn, "", stype, reclen, keyoff, keylen, iomem)
	}

	var nc = runtime.NumCPU()
	// readthe strings
	lns, _, err := merge.Flreadstrings(fp, int64(0), reclen, fsz)
	if err != nil {
		log.Fatalf("sortstringsfilech read %v: %v", fn, err)
	}

	parts := splitstringsslice(lns, nc)
	// create a string slice channel with capacity n parts
	inch := make(chan []string, len(parts))
	var wg sync.WaitGroup
	wg.Add(len(parts))

	for i := range parts {
		go func() {
			defer wg.Done()
			// log.Printf("sortstringsfilech %v", i)
			sortstringsslicech(parts[i], stype, reclen, keyoff, keylen, inch)
		}()
	}

	wg.Wait()
	tparts := make([][]string, len(parts))

	for i := range tparts {
		tparts[i] = <-inch
	}

	merge.Mergestringsparts(ofn, reclen, keyoff, keylen, tparts)
}
