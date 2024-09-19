// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sorts

import (
	"fmt"
)

const debug = false

func rsort2sa(x []string, reclen, keyoff, keylen int) {
	rsort2stringsarray(x, make([]string, len(x)), reclen, keyoff, keylen, 0)
}

func rsort2stringsarray(x, tmp []string, reclen, keyoff, keylen, offset int) {
Loop:
	// divert
	if len(x) < 16 {
		//isort(x, offset)
		ginsertionsort(x)
		return
	}

	// tally
	var counts, end [257]int
	cmin := 256
	cmax := 1
	for _, s := range x {
		c := 0
		if offset < len(s) {
			c = int(s[offset]) + 1
		}
		counts[c]++
		if counts[c] == 1 && c > 0 {
			cmin = min(cmin, c)
			cmax = max(cmax, c)
		}
	}

	// find places
	used := counts[0]
	end[0] = used
	maxc := 0
	maxcn := 0
	for c := cmin; c <= cmax; c++ {
		n := counts[c]
		if n == 0 {
			continue
		}
		used += counts[c]
		end[c] = used
		if n > maxcn {
			maxc, maxcn = c, n
		}
	}

	if debug {
		fmt.Println("x", offset, x)
		fmt.Println(counts)
		fmt.Println(end)
	}

	// move to temp
	copy(tmp, x)

	// move to home
	for i := len(x) - 1; i >= 0; i-- {
		s := tmp[i]
		c := 0
		if offset < len(s) {
			c = int(s[offset]) + 1
		}
		//		println(c)
		end[c]--
		x[end[c]] = s
	}

	if debug {
		fmt.Println("moved:", x)
	}

	// recursively sort sections, saving largest for “tail call” goto Loop.
	// Handling the largest in this stack frame guarantees that any
	// recursive call must handle ≤ len(x)/2 elements, guaranteeing
	// a logarithmic number of recursions.
	used = counts[0]
	var last []string
	for c := cmin; c <= cmax; c++ {
		n := counts[c]
		if c > 0 && n > 1 {
			if c == maxc {
				last = x[used : used+n]
			} else {
				rsort2stringsarray(x[used:used+n], tmp, reclen, keyoff, keylen, offset+1)
			}
		}
		used += n
	}
	if last != nil {
		x = last
		offset++
		goto Loop
	}
}
