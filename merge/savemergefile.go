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
// dlim - string to delimit the byte arrays
// returns name of file written
func Savemergefile(lns [][]byte, fn string, dlim string) string {

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	nw := bufio.NewWriter(fp)
	//sz := (len(lns[0]) + len(dlim)) / 4
	//nw := bufio.NewWriterSize(fp, sz)

	for _, ln := range lns {

		nl := string(ln) + dlim
		//log.Print(nl)
		_, err := nw.WriteString(nl)
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
