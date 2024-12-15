package merge

import (
	"log"
	"os"
)

// Mergebytefiles
// merge files containing sorted records represented as byte slices
// ofn - name of output file
// reclen - record length for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// fns - list of sorted files to merge
func Mergebytefiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	kvpqbreademit(ofp, reclen, keyoff, keylen, fns)
}

// Mergestringfiles
// merge files containing sorted records represented as strings
// ofn - name of output file
// reclen - record length for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// fns - list of sorted files to merge
func Mergestringfiles(ofn string, reclen int, keyoff int, keylen int, fns []string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	kvpqsreademit(ofp, reclen, keyoff, keylen, fns)
}

// Mergebytefilesch concurrent
// merge files containing sorted records represented as byte slices
// ofn - name of output file
// reclen - record length for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// fns - list of sorted files to merge
func Mergebytefilesch(ofn string, reclen int, keyoff int, keylen int, fns []string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	kvpqbreadch(ofp, reclen, keyoff, keylen, fns)
}

// Mergestringfilesch concurrent
// merge files containing sorted records represented as strings
// ofn - name of output file
// reclen - record length for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// fns - list of sorted files to merge
func Mergestringfilesch(ofn string, reclen int, keyoff int, keylen int, fns []string) {

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	kvpqsreadch(ofp, reclen, keyoff, keylen, fns)
}
