package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/vsort/merge"
)

func sortflstringsfile(fn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) ([]string, []string, error) {
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

	var offset int64
	for {

		lns, offset, err = merge.Flreadstrings(fp, offset, reclen, iomem)
		//log.Print("sortflstringsfile Flreadstrings ", len(lns), " ", offset)

		if len(lns) == 0 {
			if offset == 0 {
				log.Fatal("sortflstringsfile offset 0 no lines ", fn)
			}
			return lns, mfiles, err
		}

		//log.Print("sortflstringsfile ", stype, " ", len(lns))
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
			log.Fatal("sortflstringsfile stype ", stype)
		}
		//log.Print("sortflstringsfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savestringmergefile(lns, mfn)
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
