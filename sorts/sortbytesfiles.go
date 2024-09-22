package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

func Sortbytesfiles(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	var err error
	var mfiles []string
	log.Printf("Sortbytesfiles ofn %s dn %s, stype %ѕ reclen %d keyoff %d keylen %d, iomem %d ", ofn, dn, stype, reclen, keyoff, keylen, iomem)
	if len(dn) == 0 {
		dn, err = initmergedir("/tmp", "sortbytesfiles")
		if err != nil {
			log.Fatal("Sortbytesfiles initmergedir ", err)
		}
		//log.Print("Sortbytesfiles initmergedir ", dn)
	}

	var fp *os.File
	if ofn != "" {
		fp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal("Sortbytesfiles open ", err)
		}
		defer fp.Close()
	} else {
		fp = os.Stdout
	}

	if len(fns) == 0 {
		//log.Print("Sortbytesfiles stdin ", reclen)
		if reclen != 0 {
			_, mfiles, err = sortflbytesfile("", "", stype, reclen, keyoff, keylen, iomem)
		} else {
			_, mfiles, err = sortvlbytesfile("", "", stype, iomem)
		}
		if err != nil && err != io.EOF {
			log.Fatal("Sortbytesfiles after sort ", err)
		}
		if len(mfiles) == 0 {
			log.Fatal("Sortbytesfiles stdin no mergefile")
		}
	} else {

		for _, fn := range fns {
			var lns [][]byte
			var mfns []string

			if reclen != 0 {
				lns, mfns, err = sortflbytesfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
			} else {
				lns, mfns, err = sortvlbytesfile(fn, dn, stype, iomem)
			}
			if err != nil && err != io.EOF {
				log.Fatal("Sortbytesfiles after sort ", err)
			}
			if len(mfns) > 0 {
				mfiles = append(mfiles, mfns...)
				continue
			} else {
				log.Fatal("Sortbytesfiles no mergefiles")
			}

			mfn := fmt.Sprintf("%s", filepath.Base(fn))
			mpath := filepath.Join(dn, mfn)
			var mf string
			mf = merge.Savebytemergefile(lns, mpath)
			if mf == "" {
				log.Fatal("Sortbytesfiles Savemergefile failed ", mpath)
			}
			mfiles = append(mfiles, mpath)
		}
	}
	//log.Println("Sortbytesfiles merging", ofn)
	//log.Println("Sortbytesfiles merging", reclen, " ", mfiles)
	merge.Mergebytefiles(ofn, reclen, keyoff, keylen, mfiles)
}
