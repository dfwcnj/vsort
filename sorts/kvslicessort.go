package sorts

import (
	"bytes"
	"cmp"
	"fmt"
	"log"
	"slices"
	"strings"
)

func kvslicessort(lns [][]byte, reclen int, keyoff int, keylen int) {
	if keyoff+keylen > reclen {
		log.Fatal("key must fall withing record boundaries")
	}
	slices.SortFunc(lns, func(a, b []byte) int {
		ak := a[keyoff : keyoff+keylen]
		bk := b[keyoff : keyoff+keylen]
		return bytes.Compare(ak, bk)
	})
}
