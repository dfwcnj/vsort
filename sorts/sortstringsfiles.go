package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

func Sortstringsfiles(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	var err error
	var mfiles []string
	//log.Print("Sortstringsfiles ofn  ", ofn)
	if len(dn) == 0 {
		dn, err = initmergedir("/tmp", "Sortstringsfiles")
		if err != nil {
			log.Fatal("Sortstringsfiles initmergedir ", err)
		}
		//log.Print("Sortstringsfiles initmergedir ", dn)
	}

	var fp *os.File
	if ofn != "" {
		fp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal("Sortstringsfiles open ", err)
		}
		defer fp.Close()
	} else {
		fp = os.Stdin
	}

	if len(fns) == 0 {
		log.Println("Sortstringsfiles stdin ", reclen)
		if reclen != 0 {
			_, mfiles, err = sortflstringfile("", "", stype, reclen, keyoff, keylen, iomem)
		} else {
			_, mfiles, err = sortvlstringfile("", "", stype, iomem)
		}
		if err != nil && err != io.EOF {
			log.Fatal("Sortstringsfiles after sort ", err)
		}
		if len(mfiles) == 0 {
			log.Fatal("Sortstringsfiles stdin no mergefile")
		}
	} else {

		for _, fn := range fns {
			var lns [][]byte
			var mfns []string

			if reclen != 0 {
				lns, mfns, err = sortflstringfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
			} else {
				lns, mfns, err = sortvlstringfile(fn, dn, stype, iomem)
			}
			if err != nil && err != io.EOF {
				log.Fatal("Sortstringsfiles after sort ", err)
			}
			if len(mfns) > 0 {
				mfiles = append(mfiles, mfns...)
				continue
			} else {
				log.Fatal("Sortstringsfiles no mergefiles")
			}

			mfn := fmt.Sprintf("%s", filepath.Base(fn))
			mpath := filepath.Join(dn, mfn)
			var mf string
			mf = merge.Savemergefile(lns, mpath)
			if mf == "" {
				log.Fatal("Sortstringsfiles Savemergefile failed ", mpath)
			}
			mfiles = append(mfiles, mpath)
		}
	}
	//log.Println("Sortstringsfiles merging", ofn)
	//log.Println("Sortstringsfiles merging", reclen, " ", mfiles)
	merge.Mergefiles(ofn, reclen, keyoff, keylen, mfiles)
}
