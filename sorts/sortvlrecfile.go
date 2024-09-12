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
func sortvlrecfile(fn string, dn string, stype string, iomem int64) ([][]byte, []string, error) {
	var lns [][]byte
	var err error
	var i int
	dlim := "\n"
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
		var offset int64
		lns, offset, err = merge.Vlreadn(fp, offset, iomem)
		log.Print("sortverlcfile Vlreadn ", len(lns), " ", offset, " ", err)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		//log.Print("sortflrecfile ", stype, " ", len(lns))
		switch stype {
		case "radix":
			rsort2a(lns)
		case "std":
			kvslicessort(lns, 0, 0, 0)
		default:
			log.Fatal("sortflrecfile stype ", stype)
		}
		// log.Print("sortvlrecfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		//log.Println("sortvlrecfile save file name ", mfn)
		f := merge.Savemergefile(lns, mfn, dlim)
		if f != mfn {
			log.Fatal("sortvlrecfile Savemergefile failed: ", mfn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		//log.Println("sortvlrecfile Savemergefile ", mfn)
		if err == io.EOF {
			//log.Print("sortvlrecfile return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
