package merge

import (
	"bufio"
	"log"
	"os"
)

// save merge file
// save key and line separated by null bute
func Savemergefile(lns [][]byte, fn string, dlim string) (string, int) {

	var mrlen int

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	nw := bufio.NewWriter(fp)

	for _, ln := range lns {

		_, err := nw.WriteString(string(ln))
		if err != nil {
			log.Fatal(err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal(err)
	}
	return fn, mrlen
}
