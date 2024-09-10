package sorts

import (
	"bytes"
)

func kvinssort(lns [][]byte, keyoff, keylen int) [][]byte {
	n := len(lns)
	if n == 1 {
		return lns
	}
	for i := 0; i < n; i++ {
		for j := i; j > 0 && bytes.Compare(lns[j-1][keyoff:keyoff+keylen], lns[j][keyoff:keyoff+keylen]) > 0; j-- {
			lns[j], lns[j-1] = lns[j-1], lns[j]
		}
	}
	return lns
}
