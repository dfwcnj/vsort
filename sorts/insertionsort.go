// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// various versions of insertion sort from slices.sort

package sorts

import (
	"bytes"
	"cmp"
	"log"
	"strings"
)

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortordered.go
// kvbinsertionsort
// sort fixed length records represented as byte slices
// lns - slice of byte slices
// reclen - record length
// keyoff - offset of key in the record
// keylen - key length
func kvbinsertionsort(lns [][]byte, reclen, keyoff, keylen int) {
	if len(lns) == 0 {
		return
	}
	var lo, hi int
	hi = len(lns)
	if keyoff > 0 || keylen > 0 {
		for i := lo + 1; i < hi; i++ {
			for j := i; j > lo && (bytes.Compare(lns[j][keyoff:keyoff+keylen], lns[j-1][keyoff:keyoff+keylen]) < 0); j-- {
				lns[j], lns[j-1] = lns[j-1], lns[j]
			}
		}
	} else {
		for i := lo + 1; i < hi; i++ {
			for j := i; j > lo && (bytes.Compare(lns[j], lns[j-1]) < 0); j-- {
				lns[j], lns[j-1] = lns[j-1], lns[j]
			}
		}
	}
}

// ginsertionsort
// generic insertion sort for cmp.Ordered compatible data
// data - slice of cmp.Ordered compatible data
func ginsertionsort[E cmp.Ordered](data []E) {
	if len(data) == 0 {
		return
	}
	var a, b int
	b = len(data)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && cmp.Less(data[j], data[j-1]); j-- {
			data[j], data[j-1] = data[j-1], data[j]
		}
	}
}

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortanyfunc.go
// ginsertionsortfunc
// generic insertion sort for cmp.Ordered compatible data
// only workѕ with strings
// data - slice of cmp.Ordered compatible data
// cmp - comparison function
func ginsertionsortfunc[E any](data []E, cmp func(a, b E) int) {
	if len(data) == 0 {
		return
	}
	a := 0
	b := len(data)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && (cmp(data[j], data[j-1]) < 0); j-- {
			data[j], data[j-1] = data[j-1], data[j]
		}
	}
}

// kvsinsertionsort
// sort a slice of fixed length strings with insertion sort
// lns - slice of strings
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
func kvsinsertionsort(lns []string, reclen, keyoff, keylen int) {
	if reclen == 0 {
		ginsertionsort(lns)
	} else {
		if keyoff+keylen > reclen {
			log.Fatal("key must fall withing record boundaries")
		}
		if keylen == 0 {
			log.Fatal("kvinsertionsort zero length key")
		}
		ginsertionsortfunc(lns, func(a, b string) int {
			ak := a[keyoff : keyoff+keylen]
			bk := b[keyoff : keyoff+keylen]
			return strings.Compare(ak, bk)
		})
	}
}
