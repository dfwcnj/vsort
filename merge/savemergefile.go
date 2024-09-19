package merge

import (
	"bufio"
	"log"
	"os"
)

// save byte merge file
// lns - array of byte arrays
// fn  - destination file for the data
//
// returns name of file written
func Savebytemergefile(lns [][]byte, fn string) string {

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal("Savebytemergefile open ", err)
	}
	defer fp.Close()
	nw := bufio.NewWriterSize(fp, 1<<16)

	for _, ln := range lns {

		//_, err := fp.Write(ln)
		_, err := nw.Write(ln)
		if err != nil {
			log.Fatal("Savebytemergefile Write ", err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal("Savebytemergefile sync ", err)
	}
	return fn
}

// save string merge file
// lns - array of strings
// fn  - destination file for the data
//
// returns name of file written
func Savestringmergefile(lns []string, fn string) string {

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal("Savestringmergefile open ", err)
	}
	defer fp.Close()
	nw := bufio.NewWriterSize(fp, 1<<16)

	for _, ln := range lns {

		//_, err := fp.WriteString(ln)
		_, err := nw.WriteString(ln)
		if err != nil {
			log.Fatal("Savestringmergefile Write ", err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal("Savestringmergefile sync ", err)
	}
	return fn
}
