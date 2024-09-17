package sorts

import (
	"bytes"
	"cmp"
)

// https://cs.opensource.google/go/go/+/refs/tags/go1.23.1:src/slices/zsortordered.go
func kvinsertionsort(lns [][]byte, reclen, keyoff, keylen int) {
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
