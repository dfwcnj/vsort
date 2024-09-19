package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

// sort fixed lengh records file
func sortflbytesfile(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64) ([][]byte, []string, error) {
	var lns [][]byte
	var err error
	var i int
	var mfiles []string

	//log.Print("sortflbytesfile ", fn, " ", dn)

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortflbytesfile open ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortflbytesfile")
		if err != nil {
			log.Fatal("sortflbytesfile initmergedir ", err)
		}
		//log.Println("sortflbytesfile initmergedir ", dn)
	}

	var offset int64
	for {
		lns, offset, err = merge.Vlreadbytes(fp, offset, iomem)
		//log.Print("sortflbytesfile vlreadbytes ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		switch stype {
		case "heap":
			kvbheapsort(lns, reclen, keyoff, keylen)
		case "insertion":
			kvbinsertionsort(lns, reclen, keyoff, keylen)
		case "merge":
			kvmergesort(lns, reclen, keyoff, keylen)
		case "radix":
			kvrsort2a(lns, reclen, keyoff, keylen)
		case "std":
			kvslicesbsort(lns, reclen, keyoff, keylen)
		default:
			log.Fatal("sortflbytesfile stype ", stype)
		}

		//log.Print("sortflbytesfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savebytemergefile(lns, mfn)
		if f != mfn {
			log.Fatal("sortflbytesfile Savemergefile failed: ", mfn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			//log.Print("sortflbytesfile return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
