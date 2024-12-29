package sorts

import (
	"log"
)

// rsortsl(lns lines, recix int)
// not a very good radix sort
// lns - [][]byte each []byte represents a line
// recix - index into the line
func rsortsl(lns [][]byte, recix int) [][]byte {
	if len(lns) == 0 {
		return lns
	}
	const THRESHOLD int = 1 << 5
	var sizes = make([]int, 256)
	var piles = make([][][]byte, 256)
	var nc int
	nl := len(lns)

	if nl == 0 {
		log.Fatal("rsortsl: 0 len lines: ", recix)
	}
	if nl < THRESHOLD {
		return inssort(lns)
	}

	// count the number of lines that will fall each pile
	for i := range lns {
		var c int
		if len(lns[i]) == 0 {
			log.Fatal("rsortsl 0 length string")
		}
		if recix >= len(lns[i]) {
			c = 0
		} else {
			c = int(lns[i][recix])
		}
		sizes[c]++
	}
	// preallocate the piles so that they don't have to be resized
	for i := range sizes {
		if sizes[i] != 0 {
			piles[i] = make([][]byte, 0, sizes[i])
		}
	}

	// deal lines into piles
	for i := range lns {
		var c int

		if len(lns[i]) == 0 {
			log.Fatal("rsortsl 0 length string")
		}
		if recix >= len(lns[i]) {
			c = 0
		} else {
			c = int(lns[i][recix])
		}
		piles[c] = append(piles[c], lns[i])
		if len(piles[c]) == 1 {
			nc++ // number of piles so far
		}
	}

	// sort the piles
	if nc == 1 {
		return inssort(lns)
	}
	for i := range piles {
		if len(piles[i]) == 0 {
			continue
		}

		// sort pile
		if len(piles[i]) < THRESHOLD {
			piles[i] = inssort(piles[i])
		} else {
			piles[i] = rsortsl(piles[i], recix+1)
		}
		nc--
		if nc == 0 {
			break
		}
	}

	// combine the sorted piles
	var slns [][]byte
	for i := range piles {
		slns = append(slns, piles[i]...)
	}
	return slns
}
