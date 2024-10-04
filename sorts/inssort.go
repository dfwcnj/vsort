package sorts

import (
	"bytes"
)

// inssort
// insertion sort for slice of byte slices used by other sorts
// undoubtedly redundant
func inssort(lns [][]byte) [][]byte {
	n := len(lns)
	if n == 1 {
		return lns
	}
	for i := 0; i < n; i++ {
		for j := i; j > 0 && bytes.Compare(lns[j-1], lns[j]) > 0; j-- {
			lns[j], lns[j-1] = lns[j-1], lns[j]
		}
	}
	return lns
}
