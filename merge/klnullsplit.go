package merge

import (
	"bytes"
)

// klnullsplit(bl)
// example function for generating a key from a byte array
// this example assumes that the line contains a key and value
// separated by a null byte
func klnullsplit(bln []byte) [][]byte {
	var bls [][]byte
	var sep = make([]byte, 1)
	// split on null byte
	bls = bytes.Split(bln, sep)
	return bls
}
