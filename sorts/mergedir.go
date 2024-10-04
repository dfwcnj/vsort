package sorts

import (
	"log"
	"os"
)

// initmergedir
// initiallize a merge directory
// tn - dirname path to merge directory
// dn - name of merge directory
// return merge directory and error
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

// makemergedir
// make a temporary directory for merge file(s)
// tn - dirname path to merge directory
// dn - name of merge directory
// return merge directory and error
func makemergedir(tn string, dn string) (string, error) {
	if dn == "" {
		dn = "vsort"
	}
	mdn, err := os.MkdirTemp(tn, dn)
	return mdn, err
}
