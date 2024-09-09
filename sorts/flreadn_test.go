package sorts

import (
	"fmt"
	"github.com/dfwcnj/govbinsort/merge"
	"github.com/dfwcnj/randomdata"
	"log"
	"os"
	"path"
	"testing"
)

func Test_flreadn(t *testing.T) {
	var l int = 32
	var r bool = false
	var e bool = false
	var lrs int64 = 1 << 20
	var iomem int64 = 1 << 30

	var lns [][]byte
	var tlns [][]byte
	var offset int64
	var err error
	var nr int

	log.Println("flreadn test")

	rsl := randomdata.Randomstrings(lrs, l, r, e)
	log.Println("flreadn test rsl ", len(rsl))

	dn, err := initmergedir("/tmp", "rdxsort")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dn)

	fn := path.Join(dn, "flreadntest")
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	for i, _ := range rsl {
		fmt.Fprint(fp, rsl[i])
		nr++
	}
	fp.Sync()

	// file length
	offset, err = fp.Seek(0, 1)
	if err != nil {
		log.Fatal(err)
	}

	// rewind file
	offset, err = fp.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}

	for {
		//log.Println("flreadn test flreadn ", fn, " ", l)
		lns, offset, err = merge.Flreadn(fp, offset, int(l), iomem)
		if len(lns) == 0 {
			break
		}
		for _, ln := range lns {
			if len(ln) != int(l) {
				log.Fatal("ln ", ln, " len ", len(ln))
			}
			//log.Print(string(ln))
		}
		tlns = append(tlns, lns...)
	}
	if len(tlns) != int(lrs) {
		log.Fatal("flreadn: expected ", lrs, " got ", len(lns))
	}
	log.Print("flreadn test passed")
}
