package sorts

func splitbytesslice(lns [][]byte, ns int) [][][]byte {
	var pl int = len(lns) / ns
	var parts = make([][][]byte, 0, ns)
	if ns < 1 || ns > len(lns) {
		return parts
	}
	var off int
	for i := 0; i < ns; i++ {
		if off >= len(lns) {
			break
		}
		if len(lns)-(off+pl) < pl/2 {
			parts = append(parts, lns[off:])
			break
		}
		parts = append(parts, lns[off:off+pl])
		off += pl
	}
	return parts
}

func splitstringsslice(lns []string, ns int) [][]string {
	var pl int = len(lns) / ns
	var parts = make([][]string, 0, ns)
	if ns < 1 || ns > len(lns) {
		return parts
	}

	var off int
	for i := 0; i < ns; i++ {
		if off >= len(lns) {
			break
		}
		if len(lns)-(off+pl) < pl/2 {
			parts = append(parts, lns[off:])
			break
		}
		parts = append(parts, lns[off:off+pl])
		off += pl
	}
	return parts
}
