package sorts

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
)

func Test_mergefiles(t *testing.T) {
	var rlen int = 32
	var r bool = true
	var e bool = false
	var nrs int64 = 1 << 20
	dlim := "\n"
	var nmf = 10
	var fns []string

	log.Print("mergefiles test")

	dn, err := initmergedir("/tmp", "somesort")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dn)

	for i := range nmf {
		var lns [][]byte

		rsl := randomdata.Randomstrings(nrs, rlen, r, e)

		for _, s := range rsl {
			ln := []byte(s)
			lns = append(lns, ln)
		}

		rsort2a(lns)

		var fn = filepath.Join(dn, fmt.Sprint("file", i))
		log.Print("mergefiles test ", fn, " ", len(dlim))
		merge.Savemergefile(lns, fn, dlim)
		fns = append(fns, fn)
	}

	mfn := "mergeout.txt"
	mpath := filepath.Join(dn, mfn)
	log.Print("merge.Mergefiles ", fns)
	merge.Mergefiles(mpath, 0, 0, 0, fns)

	mfp, err := os.Open(mpath)
	if err != nil {
		log.Fatal(err)
	}
	defer mfp.Close()

	scanner := bufio.NewScanner(mfp)
	var mlns []string
	for scanner.Scan() {
		l := scanner.Text()
		mlns = append(mlns, l)
	}
	if len(mlns) != int(nrs)*nmf {
		log.Fatal(mpath, " wanted ", int(nrs)*nmf, " got ", len(mlns))
	}
	if !slices.IsSorted(mlns) {
		log.Fatal("lines in ", mpath, " not in sort order")
	}
	log.Print("mergefiles test passed")

}
