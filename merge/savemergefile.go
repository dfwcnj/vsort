package merge

import (
	"bufio"
	"log"
	"os"
)

// save merge file
// save key and line separated by null bute
func Savemergefile(lns [][]byte, fn string, dlim string) string {

	if dlim != "\n" {
		log.Fatal("WTF?")
	}
	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	nw := bufio.NewWriter(fp)

	for _, ln := range lns {

		nl := string(ln) + dlim
		log.Print(nl)
		n, err := nw.WriteString(nl)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal(err)
	}
	return fn
}
