// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sorts

import (
	"bytes"
	"cmp"
	"log"
	"strings"
)

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortordered.go
func kvbsiftdown(data [][]byte, reclen, keyoff, keylen, lo, hi, first int) {
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

func kvbheapsort(data [][]byte, reclen, keyoff, keylen int) {
	if reclen != 0 {
		if keyoff+keylen > reclen {
			log.Fatal("kvbheapsort key must fall in record bounds")
		}
	}
	first := 0
	lo := 0
	hi := len(data)

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		kvbsiftdown(data, reclen, keyoff, keylen, i, hi, first)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data[first], data[first+i] = data[first+i], data[first]
		kvbsiftdown(data, reclen, keyoff, keylen, lo, i, first)
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

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortanyfunc.go
func gsiftdownfunc[E any](data []E, lo, hi, first int, cmp func(a, b E) int) {
	root := lo
	for {
		child := 2*root + 1
		if child >= hi {
			break
		}
		if child+1 < hi && (cmp(data[first+child], data[first+child+1]) < 0) {
			child++
		}
		if !(cmp(data[first+root], data[first+child]) < 0) {
			return
		}
		data[first+root], data[first+child] = data[first+child], data[first+root]
		root = child
	}
}

func gheapsortfunc[E any](data []E, cmp func(a, b E) int) {
	first := 0
	lo := 0
	hi := len(data)

	// Build heap with greatest element at top.
	for i := (hi - 1) / 2; i >= 0; i-- {
		gsiftdownfunc(data, i, hi, first, cmp)
	}

	// Pop elements, largest first, into end of data.
	for i := hi - 1; i >= 0; i-- {
		data[first], data[first+i] = data[first+i], data[first]
		gsiftdownfunc(data, lo, i, first, cmp)
	}
}

func kvsheapsort(lns []string, reclen, keyoff, keylen int) {
	if reclen == 0 {
		gheapsort(lns)
	} else {
		if keyoff+keylen > reclen {
			log.Fatal("key must fall withing record boundaries")
		}
		if keylen == 0 {
			log.Fatal("kvsheapsort zero length key")
		}
		gheapsortfunc(lns, func(a, b string) int {
			ak := a[keyoff : keyoff+keylen]
			bk := b[keyoff : keyoff+keylen]
			return strings.Compare(ak, bk)
		})
	}
}
