package sorts

import (
	"fmt"
	"github.com/dfwcnj/govbinsort/merge"
	"io"
	"log"
	"os"
	"path/filepath"
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

		if err == io.EOF && len(mfiles) == 0 {
			return lns, mfiles, err
		}
		if len(lns) == 0 {
			return lns, mfiles, err
		}

		// XXX if keyoff !!keylen switch stype else switch stype
		log.Fatal("if keyoff !!keylen switch stype else switch stype")
		dorsort2a(lns, reclen, keyoff, keylen)

		if offset > 0 && len(lns) > 0 {
			mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
			fn = merge.Savemergefile(lns, mfn, dlim)
			if fn == "" {
				log.Fatal("Savemergefile failed: ", fn, " ", dn)
			}
			mfiles = append(mfiles, mfn)
		}
		if err == io.EOF {
			return lns, mfiles, err
		}

		i++

	}
}
