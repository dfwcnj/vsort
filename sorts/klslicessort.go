package sorts

import (
	"bytes"
	"github.com/dfwcnj/govbinsort/types"
	"slices"
)

func kllinescmp(a, b kvalline) int {
	return bytes.Compare(a.key, b.key)
}

func Klslicessort(lns kvallines) {
	slices.SortFunc(lns, kllinescmp)
}
