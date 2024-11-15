package sorts

import (
	"log"
	"sync"

	"github.com/dfwcnj/vsort/merge"
)

type mflst struct {
	mfls []string
	err  error
}

var rchan chan mflst

// sortbytesfilech
// go routine to sort fixed length bytes records files
// func sortflbytesfilech(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst, wg *sync.WaitGroup) {
func sortflbytesfilech(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst) {

	var r mflst

	_, mfns, err := sortflbytesfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
	if len(mfns) == 0 {
		log.Fatal("sortflbytesfilechan no mergefiles")
	}
	r.mfls = mfns
	r.err = err
	// log.Print("sortflbytesfilechan finished")
	res <- r
}

// sortvlbytesfilech
// go routine to sort variable length bytes records files
// func sortvlbytesfilech(fn string, dn string, stype string, iomem int64, res chan mflst, wg *sync.WaitGroup) {
func sortvlbytesfilech(fn string, dn string, stype string, iomem int64, res chan mflst) {

	var r mflst

	_, mfns, err := sortvlbytesfile(fn, dn, stype, iomem)
	if len(mfns) == 0 {
		log.Fatal("sortvlbytesfilechan no mergefiles")
	}
	r.mfls = mfns
	r.err = err
	// log.Print("sortvlbytesfilechan finished")
	res <- r
}

// sortflstringsfilech
// go routine to sort fixed length string records files
// func sortflstringsfilechan(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst, wg *sync.WaitGroup) {
func sortflstringsfilech(fn string, dn string, stype string, reclen, keyoff, keylen int, iomem int64, res chan mflst) {

	var r mflst

	_, mfns, err := sortflstringsfile(fn, dn, stype, reclen, keyoff, keylen, iomem)
	if len(mfns) == 0 {
		log.Fatal("sortflstringsfilechan no mergefiles")
	}
	r.mfls = mfns
	r.err = err
	// log.Print("sortflstringsfileschan finished")
	res <- r
}

// sortvlstringsfilech
// go routine to sort variable length string records files
// func sortvlstringsfilechan(fn string, dn string, stype string, iomem int64, res chan mflst, wg *sync.WaitGroup) {
func sortvlstringsfilech(fn string, dn string, stype string, iomem int64, res chan mflst) {

	var r mflst

	_, mfns, err := sortvlstringsfile(fn, dn, stype, iomem)
	if len(mfns) == 0 {
		log.Fatal("sortvlstringsfilechan no mergefiles")
	}
	r.mfls = mfns
	r.err = err
	// log.Print("sortvlstringsfileschan finished")
	res <- r
}

// Sortbytesfilesch
// sort bytes files using go routines and channels
func Sortbytesfilesch(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	var mfiles []string

	rchan = make(chan mflst, len(fns))
	defer close(rchan)

	var wg sync.WaitGroup
	wg.Add(len(fns))

	for _, fn := range fns {

		// log.Printf("Sortbytesfilesch sorting %s", fn)
		if reclen != 0 {
			go func() {
				defer wg.Done()
				sortflbytesfilech(fn, dn, stype, reclen, keyoff, keylen, iomem, rchan)
			}()
		} else {
			go func() {
				defer wg.Done()
				sortvlbytesfilech(fn, dn, stype, iomem, rchan)
			}()
		}

	}
	wg.Wait()

	i := 0
	for {
		if i == len(fns) {
			break
		}
		mc, ok := <-rchan
		if ok == false {
			break
		}
		mfiles = append(mfiles, mc.mfls...)
		// log.Printf("Sortbytesfilesch appending %d files", len(mc.mfls))
		i++
	}

	// log.Printf("sortbytesfilesch ofn %v merging %v", ofn, mfiles)
	merge.Mergebytefiles(ofn, reclen, keyoff, keylen, mfiles)
}

// Sortstringsfilesch
// sort strings files using go routines and channels
func Sortstringsfilesch(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	var mfiles []string

	var rchan = make(chan mflst, len(fns))
	defer close(rchan)

	var wg sync.WaitGroup
	wg.Add(len(fns))

	for _, fn := range fns {

		// log.Printf("Sortbytesfilesch sorting %s", fn)
		if reclen != 0 {
			go func() {
				defer wg.Done()
				sortflstringsfilech(fn, dn, stype, reclen, keyoff, keylen, iomem, rchan)
			}()
		} else {
			go func() {
				defer wg.Done()
				sortvlstringsfilech(fn, dn, stype, iomem, rchan)
			}()
		}
	}
	wg.Wait()

	i := 0
	for {
		if i == len(fns) {
			break
		}
		mc, ok := <-rchan
		if ok == false {
			break
		}
		mfiles = append(mfiles, mc.mfls...)
		// log.Printf("Sortbytesfilesch appending %d files", len(mc.mfls))
		i++
	}

	// log.Printf("sortbytesfilesch ofn %v merging %v", ofn, mfiles)
	merge.Mergestringfiles(ofn, reclen, keyoff, keylen, mfiles)

}
