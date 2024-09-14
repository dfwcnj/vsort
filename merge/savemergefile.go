package merge

import (
	"bufio"
	"log"
	"os"
)

// save merge file
// lns - array of byte arrays
// fn  - destination file for the data
//
// returns name of file written
func Savemergefile(lns [][]byte, fn string) string {

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal("Savemergefile open ", err)
	}
	defer fp.Close()
	nw := bufio.NewWriterSize(fp, 1<<16)

	for _, ln := range lns {

		//_, err := fp.Write(ln)
		_, err := nw.Write(ln)
		if err != nil {
			log.Fatal("Savemergefile Write ", err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal("Savemergefile sync ", err)
	}
	return fn
}
