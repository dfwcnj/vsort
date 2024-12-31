package sorts

import (
	"sync"

	"github.com/dfwcnj/vsort/merge"
)

// CSortbytesfilesch
// sort bytes files using go routines and channels
// fns - names of files to sort
// ofn - output filename
// dn  - output directろry
// stype - sort tyoe
// reclen - record length including any separator
// keyoff - offset of sort key
// keylen - length of sort key
// iomem  - size limit for each sort chunk
func CSortbytesfilesch(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	// log.Printf("CSortbytesfilesch ofn %v dn %v", ofn, dn)

	var mfiles []string

	rchan = make(chan mflst, len(fns))
	defer close(rchan)

	var wg sync.WaitGroup
	wg.Add(len(fns))

	for _, fn := range fns {
		// log.Printf("CSortbytesfilesch %v %v %v %v", i, fn, stype, reclen)

		go func() {
			defer wg.Done()
			sortbytesfilechshim(fn, dn, stype, reclen, keyoff, keylen, iomem, rchan)
		}()

	}
	wg.Wait()

	i := 0
	for {
		if i == len(fns) {
			break
		}
		mc, ok := <-rchan
		if !ok {
			break
		}
		mfiles = append(mfiles, mc.mfls...)
		// log.Printf("CSortbytesfilesch appending %d files", len(mc.mfls))
		i++
	}

	// log.Printf("csortbytesfilesch ofn %v merging %v", ofn, mfiles)
	merge.Mergebytefiles(ofn, reclen, keyoff, keylen, mfiles)
}

// CSortstringsfilesch
// sort strings files using go routines and channels
// fns - names of files to sort
// ofn - output filename
// dn  - output directろry
// stype - sort tyoe
// reclen - record length including any separator
// keyoff - offset of sort key
// keylen - length of sort key
// iomem  - size limit for each sort chunk
func CSortstringsfilesch(fns []string, ofn string, dn string, stype string, reclen int, keyoff int, keylen int, iomem int64) {

	// log.Printf("CSortstringsfilesch ofn %v dn %v", ofn, dn)

	var mfiles []string

	var rchan = make(chan mflst, len(fns))
	defer close(rchan)

	var wg sync.WaitGroup
	wg.Add(len(fns))

	for _, fn := range fns {
		// log.Printf("CSortstringsfilesch %v %v %v %v", i, fn, stype, reclen)

		go func() {
			defer wg.Done()
			sortstringsfilechshim(fn, dn, stype, reclen, keyoff, keylen, iomem, rchan)
		}()
	}
	wg.Wait()

	i := 0
	for {
		if i == len(fns) {
			break
		}
		mc, ok := <-rchan
		if !ok {
			break
		}
		mfiles = append(mfiles, mc.mfls...)
		// log.Printf("CSortstringsfilesch appending %d files", len(mc.mfls))
		i++
	}

	// log.Printf("csortstringsfilesch ofn %v merging %v", ofn, mfiles)
	merge.Mergestringfiles(ofn, reclen, keyoff, keylen, mfiles)

}
