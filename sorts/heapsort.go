package sorts

import (
	"bytes"
	"cmp"
	"log"
)

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortordered.go
func kvsiftdown(data [][]byte, reclen, keyoff, keylen, lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}

		if keyoff > 0 || keylen > 0 {
			if child+1 < hi && (bytes.Compare(data[first+child][keyoff:keyoff+keylen], data[first+child+1][keyoff:keyoff+keylen]) < 0) {
				child++
			}
			if !(bytes.Compare(data[first+root][keyoff:keyoff+keylen], data[first+child][keyoff:keyoff+keylen]) < 0) {
				return
			}
		} else {
			if child+1 < hi && (bytes.Compare(data[first+child], data[first+child+1]) < 0) {
				child++
			}
			if !(bytes.Compare(data[first+root], data[first+child]) < 0) {
				return
			}
		}
		data[first+root], data[first+child] = data[first+child], data[first+root]
		root = child
	}
}

func kvheapsort(data [][]byte, reclen, keyoff, keylen int) {
	if reclen != 0 {
		if keyoff+keylen > reclen {
			log.Fatal("kvheapsort key must fall in record bounds")
		}
	}
	first := 0
	lo := 0
	hi := len(data)

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		kvsiftdown(data, reclen, keyoff, keylen, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data[first], data[first+i] = data[first+i], data[first]
		kvsiftdown(data, reclen, keyoff, keylen, lo, i, first)
	}
}

func gsiftdown[E cmp.Ordered](data []E, lo, hi, first int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && cmp.Less(data[first+child], data[first+child+1]) {
			child++
		}
		if !cmp.Less(data[first+root], data[first+child]) {
			return
		}
		data[first+root], data[first+child] = data[first+child], data[first+root]
		root = child
	}
}

func gheapsort[E cmp.Ordered](data []E) {
	first := 0
	lo := 0
	hi := len(data)

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		gsiftdown(data, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data[first], data[first+i] = data[first+i], data[first]
		gsiftdown(data, lo, i, first)
	}
}
