package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"

	"github.com/dfwcnj/govbinsort/merge"
)

// sort variable lengh records file
func sortvlstringsfile(fn string, dn string, stype string, iomem int64) ([]string, []string, error) {
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
		lns, offset, err = merge.Vlreadstrings(fp, offset, iomem)
		//log.Print("sortvlstringfile vlreadstrings ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		switch stype {
		case "heap":
			gheapsort(lns)
		case "insertion":
			ginsertionsort(lns)
		case "merge":
			gmergesort(lns)
		case "radix":
			rsort2sa(lns, 0, 0, 0)
		case "std":
			slices.Sort(lns)
		default:
			log.Fatal("sortvlstringfile stype ", stype)
		}

		//log.Print("sortvlstringfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savestringmergefile(lns, mfn)
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