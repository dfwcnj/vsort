package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

func dosortflrecfile(fn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) ([][]byte, []string, error) {
	var lns [][]byte
	var offset int64
	var err error
	var dlim string
	dlim = ""
	var i int
	var mfiles []string

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortflrecfile ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("", "somesort")
		if err != nil {
			log.Fatal(err)
		}
	}

	for {

		lns, offset, err = merge.Flreadn(fp, offset, reclen, iomem)
		//log.Print("dosortflrecfile Flreadn ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		//log.Print("dosortflrecfile ", stype, " ", len(lns))
		switch stype {
		case "radix":
			dorsort2a(lns, reclen, keyoff, keylen)
		case "std":
			kvslicessort(lns, reclen, keyoff, keylen)
		default:
			log.Fatal("dosortflrecfile stype ", stype)
		}

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		fn = merge.Savemergefile(lns, mfn, dlim)
		if fn == "" {
			log.Fatal("Savemergefile failed: ", fn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			return lns, mfiles, err
		}

		i++

	}
}
