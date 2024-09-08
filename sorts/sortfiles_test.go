package sorts

import (
	"bufio"
	"fmt"
	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
	"log"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func Test_sortfiles(t *testing.T) {
	var l int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	//var iomem int64 = 1 << 29
	var iomem int64 = 1<<24 + 1<<20
	var nmf = 10
	var dlim string
	dlim = "\n"

	log.Print("sortfiles test")

	dn, err := initmergedir("/tmp", "somesort")

	var fns []string
	for i := range nmf {
		var lns [][]byte

		rsl := randomdata.Randomstrings(nrs, l, r, e)
		for _, s := range rsl {
			ln := []byte(s)
			lns = append(lns, ln)
		}
		if len(lns) != int(nrs) {
			log.Fatal("sortfiles test before sort wanted len ", l, " got ", len(lns))
		}

		dorsort2a(lns, 0, 0, 0)
		var fn = filepath.Join(dn, fmt.Sprint("sortfilestest", i))
		//log.Println("saving file", i)
		merge.Savemergefile(lns, fn, dlim)
		fns = append(fns, fn)
	}

	mfn := "mergeout.txt"
	mpath := filepath.Join(dn, mfn)

	DoSortfiles(fns, mpath, dn, "std", 0, 0, 0, iomem)

	mfp, err := os.Open(mpath)
	if err != nil {
		log.Fatal("sortfiles test ", err)
	}
	defer mfp.Close()

	scanner := bufio.NewScanner(mfp)
	var mlns []string
	for scanner.Scan() {
		l := scanner.Text()
		mlns = append(mlns, l)
	}
	if len(mlns) != int(nrs)*nmf {
		log.Fatal("sortfiles test n wanted ", int(nrs)*nmf, " got ", len(mlns))
	}
	if !slices.IsSorted(mlns) {
		log.Fatal("sortfiles test lines in ", mfn, " not in sort order")
	}
	log.Print("sortfiles test passed")

}
