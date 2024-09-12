package sorts

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
)

func Sortfiles(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	var lns [][]byte
	var err error
	var mfiles []string
	var dlim string = ""
	if reclen == 0 {
		dlim = "\n"
	}
	//log.Printf("sortfiles ofn %s\n", ofn)
	if len(dn) == 0 {
		dn, err = initmergedir("", "somesort")
		if err != nil {
			log.Fatal("Sortfiles initmergedir ", err)
		}
	}

	fp := os.Stdout
	if ofn != "" {
		fp, err := os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Fatal(err)
		}
		defer fp.Close()
	}
	nw := bufio.NewWriter(fp)

	if len(fns) == 0 {
		log.Println("sortfiles stdin ", reclen)
		if reclen != 0 {
			lns, mfiles, err = sortflrecfile("", "", stype, reclen, keyoff, keylen, iomem)
		} else {
			lns, mfiles, err = sortvlrecfile("", "", stype, iomem)
		}
		if err != nil && err != io.EOF {
			log.Fatal("sortfiles after sort ", err)
		}
		if len(mfiles) > 0 {
			merge.Mergefiles(ofn, reclen, keyoff, keylen, mfiles)
			return
		}

		for _, ln := range lns {

			//_, err := fp.Write(ln)
			_, err := nw.WriteString(string(ln))
			if err != nil {
				log.Fatal("sortfiles writestring ", err)
			}
		}
		err := nw.Flush()
		if err != nil {
			log.Fatal("sortfiles flush ", err)
		}

		return
	}

	for _, fn := range fns {
		var lns [][]byte
		var mfns []string

		log.Print("sortfiles sorting ", fn, "", reclen)
		if reclen != 0 {
			lns, mfns, err = sortflrecfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
		} else {
			lns, mfns, err = sortvlrecfile(fn, dn, stype, iomem)
		}
		if err != nil && err != io.EOF {
			log.Fatal("sortfiles after sort ", err)
		}
		if len(mfns) > 0 {
			log.Print("sortfiles appending ", mfns)
			mfiles = append(mfiles, mfns...)
			continue
		}

		mfn := fmt.Sprintf("%s", filepath.Base(fn))
		mpath := filepath.Join(dn, mfn)
		//log.Print("sortfiles saving merge file ", mpath)
		var mf string
		mf = merge.Savemergefile(lns, mpath, dlim)
		if mf == "" {
			log.Fatal("sortfiles Savemergefile failed ", mpath)
		}
		log.Print("sortfiles appending singleton ", mpath)
		mfiles = append(mfiles, mpath)
	}
	//log.Println("sortfiles merging", ofn)
	merge.Mergefiles(ofn, reclen, keyoff, keylen, mfiles)
}
