package merge

import (
	"log"
	"os"
)

// KVmergebytefiles
// merge sorted files with records represented as byte slices
// ofn - name of destination file
// reclen - reclen if fixed length
// keyoff - offset of key in fixed length record if any
// keylen = length of key in fixed length record if any
// fns - sorted files to merge
func KVmergebytefiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {
	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	kvpqbreademit(ofp, reclen, keyoff, keylen, fns)
}

// KVmergestringfiles
// merge sorted files with records represented as strings
// ofn - name of destination file
// reclen - reclen if fixed length
// keyoff - offset of key in fixed length record if any
// keylen = length of key in fixed length record if any
// fns - sorted files to merge
func KVmergestringfiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {
	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	kvpqsreademit(ofp, reclen, keyoff, keylen, fns)
}
