package sorts

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dfwcnj/randomdata"
	"github.com/dfwcnj/vsort/merge"
)

func Test_mergestringfiles(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 2, 2)
	bools[0] = true
	bools[1] = false
	var nrs int64 = 1 << 20

	var nmf = 10

	for _, r := range bools {
		log.Print("mergestringfiles test ", r)
		var fns []string
		dn, err := initmergedir("/tmp", "mergestringfilestest")
		if err != nil {
			log.Fatal("mergestringfiles test initmergedir ", err)
		}
		//log.Print("mergestringfiles test initmergedir ", dn)

		for i := range nmf {

			lns := randomdata.Randomstrings(nrs, rlen, r)
			// random length strings must be newline delimited
			if r == true {
				for i, _ := range lns {
					lns[i] = lns[i] + "\n"
				}
			}

			rsort2sa(lns, 0, 0, 0)

			var fn = filepath.Join(dn, fmt.Sprint("file", i))
			merge.Savestringmergefile(lns, fn)
			fns = append(fns, fn)
		}

		mfn := "mergeout.txt"
		mpath := filepath.Join(dn, mfn)
		//log.Print("merge.Mergestringfiles ", fns)

		if r == true {
			merge.Mergestringfiles(mpath, 0, 0, 0, fns)
		} else {
			merge.Mergestringfiles(mpath, rlen, 0, 0, fns)
		}

		mfp, err := os.Open(mpath)
		if err != nil {
			log.Fatal("mergestringfiles test open ", err)
		}
		defer mfp.Close()

		//log.Print("counting merged records")
		var mlns []string
		if r == true {
			scanner := bufio.NewScanner(mfp)
			for scanner.Scan() {
				l := scanner.Text()
				mlns = append(mlns, l)
			}
		} else {
			for {
				ln := make([]byte, rlen)
				_, err := io.ReadFull(mfp, ln)
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatal("mergestringfiles test  readfull ", err)
				}
				mlns = append(mlns, string(ln))
			}
		}
		if len(mlns) != int(nrs)*nmf {
			t.Fatal("mergestringfiles test ", mpath, " wanted ", int(nrs)*nmf, " got ", len(mlns))
		}
		if !slices.IsSorted(mlns) {
			t.Fatal("mergestringfiles test ", mpath, " not in sort order")
		}
	}
	log.Print("mergestringfiles test passed")

}
