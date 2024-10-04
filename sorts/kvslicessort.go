package sorts

import (
	"bytes"
	"log"
	"slices"
	"strings"
)

// kvslicesbsort
// sort fixed length byte slices using slices.Sort
// lns - slice of byte slices
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
func kvslicesbsort(lns [][]byte, reclen int, keyoff int, keylen int) {
	if reclen == 0 {
		slices.SortFunc(lns, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
	} else {
		if keyoff+keylen > reclen {
			log.Fatal("key must fall withing record boundaries")
		}
		if keylen == 0 {
			log.Fatal("kvslicesbsort zero length key")
		}
		slices.SortFunc(lns, func(a, b []byte) int {
			ak := a[keyoff : keyoff+keylen]
			bk := b[keyoff : keyoff+keylen]
			return bytes.Compare(ak, bk)
		})
	}
}

// kvslicesssort
// sort fixed length strings using slices.Sort
// lns - slice of strings
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
func kvslicesssort(lns []string, reclen int, keyoff int, keylen int) {
	if reclen == 0 {
		slices.Sort(lns)
	} else {
		if keyoff+keylen > reclen {
			log.Fatal("key must fall withing record boundaries")
		}
		if keylen == 0 {
			log.Fatal("kvslicesssort zero length key")
		}
		slices.SortFunc(lns, func(a, b string) int {
			ak := a[keyoff : keyoff+keylen]
			bk := b[keyoff : keyoff+keylen]
			return strings.Compare(ak, bk)
		})
	}
}
