package sorts

import (
	"fmt"
	"github.com/dfwcnj/govbinsort/merge"
	"io"
	"log"
	"os"
	"path/filepath"
)

// sort variable lengh records file
func dosortvlrecfile(fn string, dn string, stype string, reclen int,
	keyoff int, keylen int, iomem int64) ([][]byte, []string, error) {
	var offset int64
	var lns [][]byte
	var err error
	var i int
	var dlim string
	dlim = ""
	var mfiles []string

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortvlrecfile ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("", "somesort")
		if err != nil {
			log.Fatal(err)
		}
		// log.Println("sortvlrecfile dn ", dn)
	}

	for {
		lns, offset, err = merge.Vlreadn(fp, offset, iomem)

		if err == io.EOF && len(mfiles) == 0 {
			return lns, mfiles, err
		}
		//log.Println("sortvlrecfile vlreadn lns ", len(lns))
		if len(lns) == 0 {
			return lns, mfiles, err
		}

		dorsort2a(lns, reclen, keyoff, keylen)
		//log.Println("sortvlrecfile lns ", len(lns))

		if offset > 0 && len(lns) > 0 {
			mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
			f := merge.Savemergefile(lns, mfn, dlim)
			if f == "" {
				log.Fatal("Savemergefile failed: ", mfn, " ", dn)
			}
			mfiles = append(mfiles, mfn)
			//log.Println("sortvlrecfile Savemergefile ", mfn)
		}
		if err == io.EOF {
			return lns, mfiles, err
		}
		i++

	}
}