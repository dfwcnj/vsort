package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/vsort/merge"
)

// sortflstringsfile
// sort fixed lengh records file using string representation
// fn - file to sort
// dn - work directory
// stype - sort algorithm heap insertion merge radix std(slices.sort)
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
// returns slice of strings, merge file list, error
func sortflstringsfile(fn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) ([]string, []string, error) {
	var lns []string
	var err error
	var i int
	var mfiles []string

	// log.Printf("sortflstringsfile fn %v dn %v, stype %v reclen %v keyoff %v keylen %v, iomem %v ", fn, dn, stype, reclen, keyoff, keylen, iomem)

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
			return lns, mfiles, err
		}

		//log.Print("sortflstringsfile ", stype, " ", len(lns))
		switch stype {
		case "heap":
			kvsheapsort(lns, reclen, keyoff, keylen)
		case "insertion":
			kvsinsertionsort(lns, reclen, keyoff, keylen)
		case "merge":
			// log.Printf("sortflstringsfile kvsmergesort lns %v, reclen %v, keyoff %v keylen %v", len(lns), reclen, keyoff, keylen)
			lns = kvsmergesort(lns, reclen, keyoff, keylen)
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
