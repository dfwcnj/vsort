package merge

import (
	"log"
	"os"
)

func Mergefiles(ofn string, reclen int, fns []string) {
	// log.Print("multi step merge not implemented")

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	pqreademit(ofp, reclen, nil, fns)
}
