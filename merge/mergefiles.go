package merge

import (
	"log"
	"os"
)

func Mergebytefiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	kvpqbreademit(ofp, reclen, keyoff, keylen, fns)
}

func Mergestringfiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	kvpqsreademit(ofp, reclen, keyoff, keylen, fns)
}
