package merge

import (
	"log"
	"os"
)

func Mergebytesparts(ofn string, reclen, keyoff, keylen int, parts [][][]byte) {
	// log.Printf("mergebytesparts ofn %v reclen %v, keyoff %v keylen %v nparts %v", ofn, reclen, keyoff, keylen, len(parts))
	var err error

	ofp := os.Stdout

	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Mergebytesparts %v open %v", ofn, err)
		}
		defer ofp.Close()
	}

	kvpqbsliceemit(ofp, reclen, keyoff, keylen, parts)
}

func Mergestringsparts(ofn string, reclen, keyoff, keylen int, parts [][]string) {
	// log.Printf("mergestringsparts ofn %v reclen %v, keyoff %v keylen %v nparts %v", ofn, reclen, keyoff, keylen, len(parts))

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Mergestringsparts %v open %v", ofn, err)
		}
		defer ofp.Close()
	}

	kvpqssliceemit(ofp, reclen, keyoff, keylen, parts)
}
