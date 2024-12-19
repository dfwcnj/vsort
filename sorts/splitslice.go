package sorts

import (
	"log"
)

func splitbytesslice(lns [][]byte, ns int) [][][]byte {
	var parts = make([][][]byte, 0, ns)

	if len(lns) == 0 {
		log.Fatal("splitbytesslice zero length slice of byte slices")
		return parts
	}
	if ns < 1 || ns > len(lns) {
		log.Print("splitbytesslice number parts out of range")
		return parts
	}

	var pl int = len(lns) / ns
	var off int
	for i := 0; i < ns; i++ {
		if i == ns-1 {
			parts = append(parts, lns[off:])
			break
		}
		parts = append(parts, lns[off:off+pl])
		off += pl
	}
	return parts
}

func splitstringsslice(lns []string, ns int) [][]string {
	var parts = make([][]string, 0, ns)
	if len(lns) == 0 {
		log.Fatal("splitbytesslice zero length slice of strings")
		return parts
	}
	if ns < 1 || ns > len(lns) {
		log.Print("splitbytesslice number parts out of range")
		return parts
	}

	var pl int = len(lns) / ns
	var off int
	for i := 0; i < ns; i++ {
		if i == ns-1 {
			parts = append(parts, lns[off:])
			break
		}
		parts = append(parts, lns[off:off+pl])
		off += pl
	}
	return parts
}
