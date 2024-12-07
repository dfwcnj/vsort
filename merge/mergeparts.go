package merge

import (
	"log"
	"os"
)

func Mergebytesparts(ofn string, reclen, keyoff, keylen int, parts [][][]byte) {
	var err error

	ofp := os.Stdout

	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Mergebytesparts %v open %v", ofn, err)
		}
		defer ofp.Close()
	}

	// log.Print("mergebytesparts kvpqbsliceemit", reclen)
	kvpqbsliceemit(ofp, reclen, keyoff, keylen, parts)
}

func Mergestringsparts(ofn string, reclen, keyoff, keylen int, parts [][]string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Mergestringsparts %v open %v", ofn, err)
		}
		defer ofp.Close()
	}

	// log.Print("mergestringsparts kvpqssliceemit ", reclen)
	kvpqssliceemit(ofp, reclen, keyoff, keylen, parts)
}
