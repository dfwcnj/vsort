package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

func sortflrecfile(fn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) ([][]byte, []string, error) {
	var lns [][]byte
	var err error
	var i int
	var mfiles []string

	//log.Print("sortflrecfile ", fn, " ", dn)

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortflrecfile ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortflrecfile")
		if err != nil {
			log.Fatal("sortflrecfile initmergedir ", err)
		}
		//log.Print("sortflrecfile initmergedir ", dn)
	}

	for {
		var offset int64

		lns, offset, err = merge.Flreadn(fp, offset, reclen, iomem)
		//log.Print("sortflrecfile Flreadn ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		//log.Print("sortflrecfile ", stype, " ", len(lns))
		switch stype {
		case "heap":
			kvheapsort(lns, reclen, keyoff, keylen)
		case "insertion":
			kvinsertionsort(lns, reclen, keyoff, keylen)
		case "merge":
			kvmergesort(lns, reclen, keyoff, keylen)
		case "radix":
			kvrsort2a(lns, reclen, keyoff, keylen)
		case "std":
			kvslicessort(lns, reclen, keyoff, keylen)
		default:
			log.Fatal("sortflrecfile stype ", stype)
		}
		//log.Print("sortflrecfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savemergefile(lns, mfn)
		if f != mfn {
			log.Fatal("Savemergefile failed: ", f, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			return lns, mfiles, err
		}

		i++

	}
}
