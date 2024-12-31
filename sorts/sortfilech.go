package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/dfwcnj/vsort/merge"
)

// sortbyteѕfilechshim
// shim to adapt sortbytesfilech to sort[fv]lbytesfilech
func sortbytesfilechshim(fn, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst) {
	ofn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, 0)))
	// log.Printf("sortbytesfilechshim ofn %v", ofn)
	sortbytesfilech(fn, ofn, stype, reclen, keyoff, keylen, iomem)
	var fns = make([]string, 1)
	fns[0] = ofn
	var r mflst
	r.mfls = fns
	r.err = nil
	res <- r
}

// sortbytesfilech
// routine to split a file into pieces to sort concurrently
func sortbytesfilech(fn, ofn string, stype string, reclen, keyoff, keylen int, iomem int64) {
	// log.Print("sortbytesfilech")
	var lns [][]byte
	var offset int64
	var err error

	fp, err := os.Open(fn)
	if err != nil {
		log.Fatalf("sortbytesfilech open %v: %v", fn, err)
	}
	finf, _ := fp.Stat()
	var fsz = finf.Size()

	// exceeds our iomem limit
	if fsz > iomem {
		// log.Fatalf("sortbytesfilech file %v too large %v", fn, fsz)
		lns, mfns, err := sortbigbytesfilech(fn, "", stype, reclen, keyoff, keylen, iomem)
		if err != nil && err != io.EOF {
			log.Fatalf("sortbytesfilech sortbigbytesfilech %v %v", fn, err)
		}
		if len(lns) != 0 {
			log.Fatalf("sortbytesfilech sortbigbytesfilech %v %v lns", fn, len(lns))
		}
		merge.Mergebytefiles(ofn, reclen, keyoff, keylen, mfns)
		return
	}

	if reclen == 0 {
		lns, offset, err = merge.Vlreadbytes(fp, int64(0), iomem)
		if !strings.HasSuffix(string(lns[0]), "\n") {
			log.Fatalf("sortbytesfilech %v %v no newline", fn, string(lns[0]))
		}
	} else {
		lns, offset, err = merge.Flreadbytes(fp, int64(0), reclen, iomem)
	}
	if err != nil && err != io.EOF {
		log.Fatalf("sortbytesfilech read %v: %v %v", fn, offset, err)
	}
	var nlns = len(lns)
	// log.Printf("sortbytesfilech %v read %v lines", fn, nlns)

	var nc = runtime.NumCPU()
	parts := splitbytesslice(lns, nc)

	var pns int
	for i := range nc {
		pns += len(parts[i])
	}
	if pns != nlns {
		log.Fatalf("sortbyteѕfilech %v splitbytesslice wanted %v got %v", fn, nlns, pns)
	}
	if reclen == 0 && !strings.HasSuffix(string(parts[0][0]), "\n") {
		log.Fatalf("sortbytesfilech split %v", string(parts[0][0]))
	}

	inch := make(chan [][]byte, nc)
	defer close(inch)

	var wg sync.WaitGroup
	wg.Add(len(parts))
	for i := range parts {
		// log.Printf("sortbytesfilech calling sortbytesslicech %v", len(parts[i]))
		go func() {
			defer wg.Done()
			sortbytesslicech(parts[i], stype, reclen, keyoff, keylen, inch)
		}()
	}
	wg.Wait()

	tparts := make([][][]byte, nc)
	// log.Printf("sortbytesfilech %v parts in tparts", len(tparts))
	var ns int
	for i := range tparts {
		lns, ok := <-inch
		tparts[i] = lns
		if !ok {
			log.Printf("sortbyteѕfilech tpart %v <- inch %v", i, ok)
		}
		ns += len(lns)
	}

	if ns != len(lns) {
		log.Fatalf("sortbytesfilech sortbytesslicech %v %v wanted %v got %v", fn, stype, nlns, ns)
	}
	if reclen == 0 && !strings.HasSuffix(string(tparts[0][0]), "\n") {
		log.Fatalf("sortbytesfilech tparts %v", string(tparts[0][0]))
	}

	merge.Mergebytesparts(ofn, reclen, keyoff, keylen, tparts)
}

// sortstringsfilechshim
// shim to adapt sortstringsfilech to sort[fv]lstringsfilech
func sortstringsfilechshim(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst) {
	ofn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, 0)))
	// log.Printf("sortstringsfilechshim ofn %v", ofn)
	sortstringsfilech(fn, ofn, stype, reclen, keyoff, keylen, iomem)
	var fns = make([]string, 1)
	fns[0] = ofn
	var r mflst
	r.mfls = fns
	r.err = nil
	res <- r
}

// sortstringsfilech
// routine to split a file into pieces to sort concurrently
func sortstringsfilech(fn, ofn string, stype string, reclen, keyoff, keylen int, iomem int64) {
	// log.Print("sortstringsfilech")
	var lns []string
	var err error

	fp, err := os.Open(fn)
	if err != nil {
		log.Fatalf("sortstringsfilech open %v: %v", fn, err)
	}
	finf, _ := fp.Stat()
	var fsz = finf.Size()

	if fsz > iomem {
		// log.Fatalf("sortstringsfilech %v too large %v", fn, fsz)
		lns, mfns, err := sortbigstringsfilech(fn, "", stype, reclen, keyoff, keylen, iomem)
		if err != nil && err != io.EOF {
			log.Fatalf("sortstringsfilech sortbigstringsfilech %v %v", fn, err)
		}
		if len(lns) != 0 {
			log.Fatalf("sortstringsfilech sortbigstringsfilech %v %v lns", fn, len(lns))
		}
		merge.Mergestringfiles(ofn, reclen, keyoff, keylen, mfns)
		return
	}

	if reclen == 0 {
		lns, _, err = merge.Vlreadstrings(fp, int64(0), iomem)
		if !strings.HasSuffix(lns[0], "\n") {
			log.Fatalf("sortstringsfilech %v %v no newline", fn, lns[0])
		}
	} else {
		lns, _, err = merge.Flreadstrings(fp, int64(0), reclen, iomem)
	}
	if err != nil && err != io.EOF {
		log.Fatalf("sortstringsfilech read %v: %v", fn, err)
	}
	var nlns = len(lns)

	var nc = runtime.NumCPU()
	parts := splitstringsslice(lns, nc)
	// log.Printf("sortstringsfilech split %v", len(parts))

	var pns int
	for i := range nc {
		pns += len(parts[i])
	}
	if pns != nlns {
		log.Fatalf("sortstringѕfilech %v splitstringsslice wanted %v got %v", fn, nlns, pns)
	}
	if reclen == 0 && !strings.HasSuffix(parts[0][0], "\n") {
		log.Fatalf("sortstringsfilech split %v", parts[0][0])
	}

	inch := make(chan []string, nc)
	defer close(inch)

	var wg sync.WaitGroup
	wg.Add(len(parts))
	for _, part := range parts {
		// log.Printf("sortstringsfilech calling %v", len(part))
		go func() {
			defer wg.Done()
			sortstringsslicech(part, stype, reclen, keyoff, keylen, inch)
		}()
	}
	wg.Wait()

	tparts := make([][]string, nc)
	// log.Printf("sortstringsfilech tparts %v", len(tparts))
	var ns int
	for i := range tparts {
		lns, ok := <-inch
		tparts[i] = lns
		if !ok {
			log.Printf("sortstringsfilech tpart %v <- inch %v", i, ok)
		}
		ns += len(lns)
	}

	if ns != len(lns) {
		log.Fatalf("sortstringsfilech sortstringsslicech %v wanted %v got %v", stype, nlns, ns)
	}
	if reclen == 0 && !strings.HasSuffix(tparts[0][0], "\n") {
		log.Fatalf("sortstringsfilech split %v", tparts[0][0])
	}

	merge.Mergestringsparts(ofn, reclen, keyoff, keylen, tparts)
}
