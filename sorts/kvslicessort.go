package sorts

import (
	"bytes"
	"log"
	"slices"
)

func kvslicessort(lns [][]byte, reclen int, keyoff int, keylen int) {
	if reclen == 0 {
		slices.SortFunc(lns, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
	} else {
		if keyoff+keylen > reclen {
			log.Fatal("key must fall withing record boundaries")
		}
		if keylen == 0 {
			keylen = reclen
		}
		slices.SortFunc(lns, func(a, b []byte) int {
			ak := a[keyoff : keyoff+keylen]
			bk := b[keyoff : keyoff+keylen]
			return bytes.Compare(ak, bk)
		})
	}
}
