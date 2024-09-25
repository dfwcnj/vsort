package sorts

import (
	"bytes"
	"cmp"
	"log"
	"strings"
)

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortordered.go
func kvbinsertionsort(lns [][]byte, reclen, keyoff, keylen int) {
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

func ginsertionsort[E cmp.Ordered](data []E) {
	var a, b int
	b = len(data)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && cmp.Less(data[j], data[j-1]); j-- {
			data[j], data[j-1] = data[j-1], data[j]
		}
	}
}

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortanyfunc.go
func ginsertionsortfunc[E any](data []E, cmp func(a, b E) int) {
	a := 0
	b := len(data)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && (cmp(data[j], data[j-1]) < 0); j-- {
			data[j], data[j-1] = data[j-1], data[j]
		}
	}
}

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
