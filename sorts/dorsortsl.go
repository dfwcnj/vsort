package sorts

import (
	"log"
)

// dorsortsl(lns lines, keylen, keyoff, recix int)
// lns - [][]byte each []byte represents a line
// recix - index into the line
func dorsortsl(lns [][]byte, keyoff int, keylen int, recix int) [][]byte {
	const THRESHOLD int = 1 << 5
	var sizes = make([]int, 256)
	var piles = make([][][]byte, 256)
	var nc int
	nl := len(lns)

	if nl == 0 {
		log.Fatal("dorsortsl: 0 len lines: ", recix)
	}
	if nl < THRESHOLD {
		return doinssort(lns, keyoff, keylen)
	}

	// count the number of lines that will fall each pile
	for i, _ := range lns {
		var c int
		if len(lns[i]) < keyoff+keylen {
			log.Fatal("key must fall within key boundaries")
		}
		key := lns[i][keyoff : keyoff+keylen]
		if recix >= len(key) {
			c = 0
		} else {
			c = int(key[recix])
		}
		sizes[c]++
	}
	// preallocate the piles so that they don't have to be resized
	for i, _ := range sizes {
		if sizes[i] != 0 {
			piles[i] = make([][]byte, 0, sizes[i])
		}
	}

	// deal lines into piles
	for i, _ := range lns {
		var c int

		if len(lns[i]) == 0 {
			log.Fatal("dorsortsl 0 length string")
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
		return doinssort(lns, keyoff, keylen)
	}
	for i, _ := range piles {
		if len(piles[i]) == 0 {
			continue
		}

		// sort pile
		if len(piles[i]) < THRESHOLD {
			piles[i] = doinssort(piles[i], keyoff, keylen)
		} else {
			piles[i] = dorsortsl(piles[i], keyoff, keylen, recix+1)
		}
		nc--
		if nc == 0 {
			break
		}
	}

	// combine the sorted piles
	var slns [][]byte
	for i, _ := range piles {
		for j, _ := range piles[i] {
			slns = append(slns, piles[i][j])
		}
	}
	return slns
}
