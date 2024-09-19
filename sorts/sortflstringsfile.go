package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

func sortflstringsfile(fn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) ([][]byte, []string, error) {
	var lns []string
	var err error
	var i int
	var mfiles []string

	//log.Print("sortflstringsfile ", fn, " ", dn)

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortflstringsfile ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortflstringsfile")
		if err != nil {
			log.Fatal("sortflstringsfile initmergedir ", err)
		}
		//log.Print("sortflstringsfile initmergedir ", dn)
	}

	for {
		var offset int64

		lns, offset, err = merge.Flreadstrings(fp, offset, reclen, iomem)
		//log.Print("sortflstringsfile Flreadstrings ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		//log.Print("sortflstringsfile ", stype, " ", len(lns))
		switch stype {
		case "heap":
			gheapsort(lns, reclen, keyoff, keylen)
		case "insertion":
			ginsertionsort(lns, reclen, keyoff, keylen)
		case "merge":
			gmergesort(lns, reclen, keyoff, keylen)
		case "radix":
			grsort2a(lns, reclen, keyoff, keylen)
		case "std":
			kvslicessort(lns, reclen, keyoff, keylen)
		default:
			log.Fatal("sortflstringsfile stype ", stype)
		}
		//log.Print("sortflstringsfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savemergefilestring(lns, mfn)
		if f != mfn {
			log.Fatal("Savemergefilestring failed: ", f, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			return lns[:0], mfiles, err
		}

		i++

	}
}
