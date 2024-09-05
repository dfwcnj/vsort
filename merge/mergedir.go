package merge

import (
	"log"
	"os"
)

func initmergedir(tn string, dn string) (string, error) {
	mdn, err := makemergedir(tn, dn)
	if err != nil {
		if os.IsExist(err) {
			os.RemoveAll(mdn)
			return makemergedir(tn, dn)
		}
		log.Fatal(err)
	}
	return mdn, err

}

func makemergedir(tn string, dn string) (string, error) {
	if dn == "" {
		dn = "somesort"
	}
	mdn, err := os.MkdirTemp(tn, dn)
	return mdn, err
}

