package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

// sort variable lengh records file
func sortvlstringfile(fn string, dn string, stype string, iomem int64) ([][]byte, []string, error) {
	var lns []string
	var err error
	var i int
	var mfiles []string

	//log.Print("sortvlstringfile ", fn, " ", dn)

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortvlstringfile open ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortvlstringfile")
		if err != nil {
			log.Fatal("sortvlstringfile initmergedir ", err)
		}
		//log.Println("sortvlstringfile initmergedir ", dn)
	}

	var offset int64
	for {
		lns, offset, err = merge.Vlreadstring(fp, offset, iomem)
		//log.Print("sortvlstringfile vlreadstring ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		switch stype {
		case "heap":
			gheapsort(lns, 0, 0, 0)
		case "insertion":
			ginsertionsort(lns, 0, 0, 0)
		case "merge":
			gmergesort(lns, 0, 0, 0)
		case "radix":
			rsort2a(lns)
		case "std":
			kvslicessort(lns, 0, 0, 0)
		default:
			log.Fatal("sortvlstringfile stype ", stype)
		}

		//log.Print("sortvlstringfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savemergefilestring(lns, mfn)
		if f != mfn {
			log.Fatal("sortvlstringfile Savemergefilestring failed: ", mfn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			//log.Print("sortvlstringfile return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
