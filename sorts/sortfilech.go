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
		if off+pl > len(lns) {
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
		if off+pl > len(lns) {
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
	log.Print("sortbytesslicech")
	switch stype {
	case "heap":
		kvbheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvbinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		lns = kvbmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		kvrsort2a(lns, reclen, keyoff, keylen)
	case "std":
		kvslicesbsort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortbytesslicech stype ", stype)
	}
	ouch <- lns
}

// sortstringsslicech
func sortstringsslicech(lns []string, stype string, reclen, keyoff, keylen int, ouch chan []string) {
	log.Print("sortstringsslicech")
	switch stype {
	case "heap":
		kvsheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvsinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		lns = kvsmergesort(lns, reclen, keyoff, keylen)
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
func sortbytesfilech(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64) {
	log.Print("sortbytesfilech")
	fp, err := os.Open(fn)
	if err != nil {
		log.Fatalf("sortbytesfilech open %v: %v", fn, err)
	}
	finf, err := fp.Stat()
	var fsz = finf.Size()

	// exceeds our iomem limit
	if fsz > iomem {
		if reclen > 0 {
			sortflbytesfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
		} else {
			sortvlbytesfile(fn, dn, stype, iomem)
		}
		return
	}

	var nc = runtime.NumCPU()
	// readthe bytes
	lns, _, err := merge.Flreadbytes(fp, int64(0), reclen, fsz)
	if err != nil {
		log.Fatalf("sortbytesfilech read %v: %v", fn, err)
	}

	var wg sync.WaitGroup
	wg.Add(nc)
	// create a byte slice channel size nc
	inch := make(chan [][]byte, nc)

	parts := splitbytesslice(lns, nc)
	for i := range parts {

		go func() {
			defer wg.Done()
			sortbytesslicech(parts[i], stype, reclen, keyoff, keylen, inch)
		}()
	}
	wg.Wait()
}

// sortstringsfilech
// routine to split a file into pieces to sort concurrently
func sortstringsfilech(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64) {
	log.Print("sortstringsfilech")
	fp, err := os.Open(fn)
	if err != nil {
		log.Fatalf("sortstringsfilech open %v: %v", fn, err)
	}
	finf, err := fp.Stat()
	var fsz = finf.Size()

	// exceeds our iomem limits
	if fsz > iomem {
		if reclen > 0 {
			sortflstringsfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
		} else {
			sortvlstringsfile(fn, dn, stype, iomem)
		}
		return
	}

	var nc = runtime.NumCPU()
	// readthe strings
	lns, _, err := merge.Flreadstrings(fp, int64(0), reclen, fsz)
	if err != nil {
		log.Fatalf("sortstringsfilech read %v: %v", fn, err)
	}

	var wg sync.WaitGroup
	wg.Add(nc)
	// create a string slice channel size nc
	inch := make(chan []string, nc)

	parts := splitstringsslice(lns, nc)
	for i := range parts {

		go func() {
			defer wg.Done()
			sortstringsslicech(parts[i], stype, reclen, keyoff, keylen, inch)
		}()
	}
	wg.Wait()
}
