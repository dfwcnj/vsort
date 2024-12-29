package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/dfwcnj/vsort/merge"
)

func sortbigbytesfilechshim(fn, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst) {
	var r mflst
	lns, fns, err := sortbigbytesfilech(fn, dn, stype, reclen, keyoff, keylen, iomem)
	if err != nil && err != io.EOF {
		log.Fatalf("sortbigbytesfileshim %v %v", fn, err)
	}
	if len(lns) != 0 {
		log.Fatalf("sortbigbytesfileshim %v lns %v", fn, len(lns))
	}
	r.mfls = fns
	r.err = err
	res <- r
}

// sortbigbytesfilech
// sort a file concurrently splitting big files into iomem chunks
// fn - file to sort
// dn - merge directory
// stype - sort type
// reclen - record length for fixed length records
// keyoff - key offset for fixed length records
// keylen - key length for fixed length records
// iomem - approximate amount of memory for sorting
func sortbigbytesfilech(fn, dn string, stype string, reclen, keyoff, keylen int, iomem int64) ([][]byte, []string, error) {
	// log.Printf("sortbigbytesfile fn %v dn %v, stype %v reclen %v keyoff %v keylen %v, iomem %v ", fn, dn, stype, reclen, keyoff, keylen, iomem)

	var lns [][]byte
	var err error
	var i int
	var mfiles []string
	var nc = runtime.NumCPU()

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortbigbytesfile open ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortbigbytesfile")
		if err != nil {
			log.Fatal("sortbigbytesfile initmergedir ", err)
		}
		//log.Println("sortbigbytesfile initmergedir ", dn)
	}

	var offset int64

	for {
		if reclen == 0 {
			lns, offset, err = merge.Vlreadbytes(fp, offset, iomem)
		} else {
			lns, offset, err = merge.Flreadbytes(fp, offset, reclen, iomem)
		}
		if err != nil && err != io.EOF {
			log.Fatalf("sortbigbytesfilech %v %v", fn, err)
		}
		// log.Print("sortflbytesfile vlreadbytes ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		parts := splitbytesslice(lns, nc)
		// log.Printf("sortbigfilech test %v offset %v parts", offset, len(parts))
		var pc int
		for i := range parts {
			// log.Printf("sortbigbytesfilech splitbytesslicepart %v %v", i, len(parts[i]))
			pc += len(parts[i])
		}
		if pc != len(lns) {
			log.Fatalf("sortbigbytesfilech splitbytesslice wanted %v got %v", len(lns), pc)
		}

		inch := make(chan [][]byte, len(parts))

		var wg sync.WaitGroup
		wg.Add(len(parts))
		for i := range parts {
			go func() {
				defer wg.Done()
				// log.Printf("sortbytesfilech part %v", i)
				sortbytesslicech(parts[i], stype, reclen, keyoff, keylen, inch)
			}()
		}
		wg.Wait()

		tparts := make([][][]byte, len(parts))
		var ns int
		for i := range tparts {
			tparts[i] = <-inch
			ns += len(parts[i])
			// log.Printf("sortbigbytesfilech tpart %v %v", i, len(tparts[i]))
		}
		if ns != len(lns) {
			log.Fatalf("sortbifbytesfilech sortbytesslicech %v %v wanted %v got %v", fn, stype, len(lns), ns)
		}

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		merge.Mergebytesparts(mfn, reclen, keyoff, keylen, tparts)
		mfiles = append(mfiles, mfn)

		// log.Print("sortbytesfile mfn ", mfn)
		if err == io.EOF {
			//log.Print("sortbigbytesfile return on EOF")
			return lns[:0], mfiles, err
		}
		i++
	}

}

func sortbigstringsfilechshim(fn, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst) {
	var r mflst
	lns, fns, err := sortbigstringsfilech(fn, dn, stype, reclen, keyoff, keylen, iomem)
	if err != nil && err != io.EOF {
		log.Fatalf("sortbigbytesfileshim %v %v", fn, err)
	}
	if len(lns) != 0 {
		log.Fatalf("sortbigbytesfileshim %v lns %v", fn, len(lns))
	}
	r.mfls = fns
	r.err = err
	res <- r
}

// sortbigtringsilech
// sort a file concurrently splitting big files into iomem chunks
// fn - file to sort
// dn - merge directory
// stype - sort type
// reclen - record length for fixed length records
// keyoff - key offset for fixed length records
// keylen - key length for fixed length records
// iomem - approximate amount of memory for sorting
func sortbigstringsfilech(fn, dn string, stype string, reclen, keyoff, keylen int, iomem int64) ([]string, []string, error) {

	// log.Printf("sortbigstringsfilech fn %v dn %v, stype %v reclen %v keyoff %v keylen %v, iomem %v ", fn, dn, stype, reclen, keyoff, keylen, iomem)
	var lns []string
	var err error
	var i int
	var mfiles []string
	var nc = runtime.NumCPU()

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortbigstringsfilech ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "bigstringsfilech")
		if err != nil {
			log.Fatal("sortbigstringsfilech initmergedir ", err)
		}
		//log.Print("sortbigstringsfilech initmergedir ", dn)
	}

	var offset int64
	for {

		if reclen == 0 {
			lns, offset, err = merge.Vlreadstrings(fp, offset, iomem)
		} else {
			lns, offset, err = merge.Flreadstrings(fp, offset, reclen, iomem)
		}
		// log.Printf("sortbigstringsfilech readstrings %v %v", len(lns), offset)
		if err != nil && err != io.EOF {
			log.Fatalf("sortbigstringsfilech %v %v", fn, err)
		}

		if len(lns) == 0 {
			// log.Print("sortbigstringsfilech return on len(lns) == 0")
			return lns, mfiles, err
		}

		parts := splitstringsslice(lns, nc)
		var pc int
		for i := range parts {
			pc += len(parts[i])
		}
		if pc != len(lns) {
			log.Fatalf("sortbigstringsfilech splitbytesslice wanted %v got %v", len(lns), pc)
		}

		// create a string slice channel with capacity n parts
		inch := make(chan []string, len(parts))
		var wg sync.WaitGroup
		wg.Add(len(parts))

		for i := range parts {
			go func() {
				defer wg.Done()
				// log.Printf("sortbigstringsfilech %v", i)
				sortstringsslicech(parts[i], stype, reclen, keyoff, keylen, inch)
			}()
		}

		wg.Wait()
		tparts := make([][]string, len(parts))

		var plns int
		for i := range tparts {
			tparts[i] = <-inch
			// log.Printf("sortbigstringsfilech tpart %v %v", i, len(tparts[i]))
			plns += len(tparts[i])
		}
		if plns != len(lns) {
			log.Fatalf("sortbigstringsfilech tpsrts wanted %v got %v", len(lns), pc)
		}
		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		merge.Mergestringsparts(mfn, reclen, keyoff, keylen, tparts)
		mfiles = append(mfiles, mfn)

		// log.Print("sortbigstringsfilech mfn ", mfn)
		if err == io.EOF {
			// log.Print("sortbigstringsfilech return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
