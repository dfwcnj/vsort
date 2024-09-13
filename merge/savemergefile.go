package merge

import (
	//"bufio"

	"bufio"
	"log"
	"os"
)

// save merge file
// lns - array of byte arrays
// fn  - destination file for the data
//
//	can be the empty string
//
// returns name of file written
func Savemergefile(lns [][]byte, fn string) string {

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal("Savemergefile open ", err)
	}
	defer fp.Close()

	nw := bufio.NewWriter(fp)

	for _, ln := range lns {

		_, err := nw.WriteString(string(ln))
		if err != nil {
			log.Fatal("Savemergefile WriteString ", err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal("Savemergefile Flush ", err)
	}
	return fn
}
