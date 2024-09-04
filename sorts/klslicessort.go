package sorts

import (
	"bytes"
	"slices"
)

func kllinescmp(a, b kvalline) int {
	return bytes.Compare(a.key, b.key)
}

func Klslicessort(lns kvallines) {
	slices.SortFunc(lns, kllinescmp)
}
