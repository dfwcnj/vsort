package sorts

// shæmelessly plagiarized from
// https://github.com/rsc/tmp/blob/master/rsort/rsort.go sortWithTmp
// without the goto, using byte slices instead of strings
// commented so that I can understand it better
// rsc calls the tmp folder throwaray but the rsort directory is good
// the rsc github directory is a great place to see an analysis of radix
// sort compared to slices.sort.
// if you are interested in radix sort, his three functions in rsort.go
// are highly instructive.
// same license as rsc code - BSD

import (
	"log"
)

func kvrsort2a(lns [][]byte, reclen, keyoff, keylen int) {
	if keylen == 0 {
		log.Fatal("kvrsort2a zero length key")
	}
	if keyoff+keylen > reclen {
		log.Fatal("kvrsort2a key must fall withing key bounds")
	}
	kvrsort2array(lns, make([][]byte, len(lns)), reclen, keyoff, keylen, 0)
}

func kvrsort2array(lns, lns2 [][]byte, reclen, keyoff, keylen, ix int) {
	if len(lns) < 16 {
		kvbinsertionsort(lns, reclen, keyoff, keylen) // insertion sort
		return
	}

	// [][]byte are sorted into bins based on byte at offset ix
	// compute bin sizes
	var sizes, ends [257]int // bin sizes and end indices
	fbin := 256              // first assignable bin
	lbin := 1                // last assignable bin
	for _, s := range lns {
		c := 0 // for shorties
		if len(s) < keyoff+keylen {
			log.Fatal("key must fall within record boundaries")
		}
		key := s[keyoff : keyoff+keylen]
		if ix < len(key) {
			c = int(key[ix]) + 1
		}
		sizes[c]++

		if sizes[c] == 1 && c > 0 { // bin is newly assigned
			fbin = min(fbin, c)
			lbin = max(lbin, c)
		}
	}

	// compute bin ends for move into bins below
	off := sizes[0] // skip unassignable bine
	ends[0] = off
	for i := fbin; i <= lbin; i++ {
		n := sizes[i]
		if n == 0 {
			continue
		}
		off += sizes[i]
		ends[i] = off // bin end
	}

	// lns2 simplifies moving [][]byte into their bins
	copy(lns2, lns)

	// move [][]byte into bins
	for i := len(lns) - 1; i >= 0; i-- {
		s := lns2[i]
		c := 0 // bin for for shorties
		if ix < len(s) {
			c = int(s[ix]) + 1 // destination bin
		}
		ends[c]--        // ends contained size, not last offset
		lns[ends[c]] = s // move to home
	}

	// recurse binning the [][]byte with the next byte offset
	off = sizes[0]
	for c := fbin; c <= lbin; c++ {
		n := sizes[c] // size of bin
		if c > 0 && n > 1 {
			kvrsort2array(lns[off:off+n], lns2, reclen, keyoff, keylen, ix+1)
		}
		off += n // offset to next bin
	}
}
