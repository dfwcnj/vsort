package sorts

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/dfwcnj/govbinsort/merge"
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
		log.Print("dosortverecfile ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		log.Print("dosortflrecfile ", stype, " ", len(lns))
		if reclen != 0 {
			switch stype {
			case "radix":
				dorsort2a(lns, reclen, keyoff, keylen)
			case "std":
				kvslicessort(lns, reclen, keyoff, keylen)
			default:
				log.Fatal("dosortflrecfile stype ", stype)
			}
		} else {
			switch stype {
			case "radix":
				rsort2a(lns)
			case "std":
				kvslicessort(lns, 0, 0, 0)
			default:
				log.Fatal("dosortflrecfile stype ", stype)
			}
		}

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		log.Println("sortvlrecfile save file name ", mfn)
		f := merge.Savemergefile(lns, mfn, dlim)
		if f == "" {
			log.Fatal("dosortvlrecfile Savemergefile failed: ", mfn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		log.Println("dosortvlrecfile Savemergefile ", mfn)
		if err == io.EOF {
			log.Print("dosortvlrecfile return on EOF")
			return lns, mfiles, err
		}
		i++

	}
}
