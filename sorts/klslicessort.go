package sorts

import (
	"bytes"
	"github.com/dfwcnj/govbinsort/merge"
	"slices"
)

func kllinescmp(a, b Kvalline) int {
	return bytes.Compare(a.key, b.key)
}

func Klslicessort(lns Kvallines) {
	slices.SortFunc(lns, kllinescmp)
}
