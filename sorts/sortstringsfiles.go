package sorts

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/vsort/merge"
)

// Sortstringsfiles
// top level sort function called by main using string representation
// fnѕ - files to sort
// ofn - output file name
// dn - directory name for work files
// stype - sort algorithm
// reclen - record length for fixed length records
// keyoff - offset of key in record
// keylen - key length
func Sortstringsfiles(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	//log.Printf("Sortstringsfiles %s %s %s %d %d %d %d", ofn, dn, stype, reclen, keyoff, keylen, iomem)

	var err error
	var mfiles []string
	//log.Print("Sortstringsfiles ofn  ", ofn)

	var fp *os.File = os.Stdout
	if ofn != "" {
		fp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal("Sortstringsfiles open ", err)
		}
		defer fp.Close()
	}

	if len(fns) == 0 {
		// log.Println("Sortstringsfiles stdin ", reclen)
		if reclen != 0 {
			_, mfiles, err = sortflstringsfile("", "", stype, reclen, keyoff, keylen, iomem)
		} else {
			_, mfiles, err = sortvlstringsfile("", "", stype, iomem)
		}
		if err != nil && err != io.EOF {
			log.Fatal("Sortstringsfiles after sort ", err)
		}
		if len(mfiles) == 0 {
			log.Fatal("Sortstringsfiles stdin no mergefile")
		}
	} else {
		if len(dn) == 0 {
			dn, err = initmergedir("/tmp", "Sortstringsfiles")
			if err != nil {
				log.Fatal("Sortstringsfiles initmergedir ", err)
			}
			//log.Print("Sortstringsfiles initmergedir ", dn)
		}

		for _, fn := range fns {
			var lns []string
			var mfns []string

			if reclen != 0 {
				lns, mfns, err = sortflstringsfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
			} else {
				lns, mfns, err = sortvlstringsfile(fn, dn, stype, iomem)
			}
			if err != nil && err != io.EOF {
				log.Fatal("Sortstringsfiles after sort ", err)
			}
			if len(lns) != 0 {
				log.Fatalf("Sortstringsfiles sort[fv]lstringsfile %v %v", fn, stype)
			}
			if len(mfns) > 0 {
				mfiles = append(mfiles, mfns...)
				continue
			} else {
				log.Fatal("Sortstringsfiles no mergefiles")
			}

			mfn := filepath.Base(fn)
			mpath := filepath.Join(dn, mfn)
			mf := merge.Savestringmergefile(lns, mpath)
			if mf == "" {
				log.Fatal("Sortstringsfiles Savestringmergefile failed ", mpath)
			}
			mfiles = append(mfiles, mpath)
		}
	}
	//log.Println("Sortstringsfiles merging", ofn)
	//log.Println("Sortstringsfiles merging", reclen, " ", mfiles)
	merge.Mergestringfiles(ofn, reclen, keyoff, keylen, mfiles)
}
