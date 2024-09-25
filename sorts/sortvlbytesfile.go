package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/vsort/merge"
)

// sort variable lengh records file
func sortvlbytesfile(fn string, dn string, stype string, iomem int64) ([][]byte, []string, error) {
	// log.Printf("sortvlbytesfile fn %v dn %v, stype %v iomem %v ", fn, dn, stype, iomem)
	var lns [][]byte
	var err error
	var i int
	var mfiles []string

	//log.Print("sortvlbytesfile ", fn, " ", dn)

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortvlbytesfile open ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortvlbytesfile")
		if err != nil {
			log.Fatal("sortvlbytesfile initmergedir ", err)
		}
		//log.Println("sortvlbytesfile initmergedir ", dn)
	}

	var offset int64
	for {
		lns, offset, err = merge.Vlreadbytes(fp, offset, iomem)
		//log.Print("sortvlbytesfile vlreadbytes ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		switch stype {
		case "heap":
			kvbheapsort(lns, 0, 0, 0)
		case "insertion":
			kvbinsertionsort(lns, 0, 0, 0)
		case "merge":
			lns = kvbmergesort(lns, 0, 0, 0)
		case "radix":
			rsort2ba(lns)
		case "std":
			kvslicesbsort(lns, 0, 0, 0)
		default:
			log.Fatal("sortvlbytesfile stype ", stype)
		}

		//log.Print("sortvlbytesfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savebytemergefile(lns, mfn)
		if f != mfn {
			log.Fatal("sortvlbytesfile Savemergefile failed: ", mfn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			//log.Print("sortvlbytesfile return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
