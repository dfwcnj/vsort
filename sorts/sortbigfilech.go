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
	log.Printf("sortbigbytesfile fn %v dn %v, stype %v reclen %v keyoff %v keylen %v, iomem %v ", fn, dn, stype, reclen, keyoff, keylen, iomem)

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
		lns, offset, err = merge.Flreadbytes(fp, offset, reclen, iomem)
		//log.Print("sortflbytesfile vlreadbytes ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		parts := splitbytesslice(lns, nc)
		// create a byte slice channel with n parts capacity
		inch := make(chan [][]byte, len(parts))
		var wg sync.WaitGroup
		wg.Add(len(parts))

		for i := range parts {
			go func() {
				defer wg.Done()
				log.Printf("sortbytesfilech %v", i)
				sortbytesslicech(parts[i], stype, reclen, keyoff, keylen, inch)
			}()
		}

		wg.Wait()
		tparts := make([][][]byte, len(parts))

		for i := range tparts {
			tparts[i] = <-inch
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

		lns, offset, err = merge.Flreadstrings(fp, offset, reclen, iomem)
		//log.Print("sortbigstringsfilech Flreadstrings ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		var nc = runtime.NumCPU()
		// readthe strings
		lns, _, err := merge.Flreadstrings(fp, int64(0), reclen, iomem)
		if err != nil {
			log.Fatalf("sortbigstringsfilech read %v: %v", fn, err)
		}

		parts := splitstringsslice(lns, nc)
		// create a string slice channel with capacity n parts
		inch := make(chan []string, len(parts))
		var wg sync.WaitGroup
		wg.Add(len(parts))

		for i := range parts {
			go func() {
				defer wg.Done()
				log.Printf("sortbigstringsfilech %v", i)
				sortstringsslicech(parts[i], stype, reclen, keyoff, keylen, inch)
			}()
		}

		wg.Wait()
		tparts := make([][]string, len(parts))

		for i := range tparts {
			tparts[i] = <-inch
		}
		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		merge.Mergestringsparts(mfn, reclen, keyoff, keylen, tparts)
		mfiles = append(mfiles, mfn)

		// log.Print("sortbigstringsfilech mfn ", mfn)
		if err == io.EOF {
			//log.Print("sortbigstringsfilech return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
