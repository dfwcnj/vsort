package merge

import (
	"log"
	"os"
)

func KVmergefiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {
	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	kvpqreademit(ofp, reclen, keyoff, keylen, fns)
}
