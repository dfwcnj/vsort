package sorts

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
)

func Test_savemergefile(t *testing.T) {
	var rlen int = 32
	var bools []bool = make([]bool, 0, 2)
	bools[0] = true
	bools[1] = false
	var e bool = false
	var nrs int64 = 1 << 20

	for _, r := range bools {
		log.Print("savemergefile test", r)
		dn, err := initmergedir("/tmp", "savemergefiletest")
		if err != nil {
			log.Fatal("savemergefile test initmergedir ", err)
		}
		defer os.RemoveAll(dn)

		for i := range 10 {
			var lns [][]byte

			rsl := randomdata.Randomstrings(nrs, rlen, r, e)
			for _, s := range rsl {
				ln := []byte(s)
				lns = append(lns, ln)
			}

			rsort2a(lns)

			if len(lns) != int(nrs) {
				log.Fatal("savemergefile test lns: before sort wanted len ", rlen, " got ", len(lns))
			}

			var fn = filepath.Join(dn, fmt.Sprint("file", i))
			if r == true {
				merge.Savemergefile(lns, fn, "\n")
			} else {
				merge.Savemergefile(lns, fn, "")
			}
			//log.Print("savemergefile test Savemergefile returned ", mf)

			fp, err := os.Open(fn)
			if err != nil {
				log.Fatal("savemergefile test open ", err)
			}
			defer fp.Close()

			var rlns []string
			if r == true {
				scanner := bufio.NewScanner(fp)
				for scanner.Scan() {
					l := scanner.Text()
					if len(l) == 0 {
						continue
					}
					rlns = append(rlns, l)
				}
			} else {
				for {
					for {
						ln := make([]byte, rlen)
						_, err := io.ReadFull(fp, ln)
						if err != nil {
							if err == io.EOF {
								break
							}
							log.Fatal("mergefiles test  readfull ", err)
						}
						rlns = append(rlns, string(ln))
					}
				}
			}
			if len(rlns) != int(nrs) {
				t.Fatal("savemergefile test failed rlns wanted ", nrs, " got ", len(rlns))
			}
		}
	}
	log.Print("savemergefile test passed")
}
