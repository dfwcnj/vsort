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
	var mfiles []string

	//log.Print("sortvlrecfile ", fn, " ", dn)

	fp := os.Stdin
	if fn != "" {
		fp, err = os.Open(fn)
		if err != nil {
			log.Fatal("sortvlrecfile open ", err)
		}
	}
	if dn == "" {
		dn, err = initmergedir("/tmp", "sortvlrecfile")
		if err != nil {
			log.Fatal("sortvlrecfile initmergedir ", err)
		}
		//log.Println("sortvlrecfile initmergedir ", dn)
	}

	var offset int64
	for {
		lns, offset, err = merge.Vlreadn(fp, offset, iomem)
		//log.Print("sortvlrecfile vlreadn ", len(lns), " ", offset)

		if len(lns) == 0 {
			return lns, mfiles, err
		}

		switch stype {
		case "radix":
			rsort2a(lns)
		case "std":
			kvslicessort(lns, 0, 0, 0)
		default:
			log.Fatal("sortvlrecfile stype ", stype)
		}
		//log.Print("sortvlrecfile sorted ", len(lns))

		mfn := filepath.Join(dn, filepath.Base(fmt.Sprintf("%s%d", fn, i)))
		f := merge.Savemergefile(lns, mfn)
		if f != mfn {
			log.Fatal("sortvlrecfile Savemergefile failed: ", mfn, " ", dn)
		}
		mfiles = append(mfiles, mfn)
		if err == io.EOF {
			//log.Print("sortvlrecfile return on EOF")
			return lns[:0], mfiles, err
		}
		i++

	}
}
