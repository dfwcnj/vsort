package sorts

// slightly modified version of
// https://gist.github.com/julianshen/3940045

import (
	"bytes"
	"cmp"
	"log"
)

func kvbmerge(ldata, rdata [][]byte, reclen, keyoff, keylen int) [][]byte {
	var lidx, ridx int
	result := make([][]byte, len(ldata)+len(rdata))

	if keyoff > 0 || keylen > 0 {
		for i := 0; i < cap(result); i++ {
			switch {
			case lidx >= len(ldata):
				result[i] = rdata[ridx]
				ridx++
			case ridx >= len(rdata):
				result[i] = ldata[lidx]
				lidx++
			case bytes.Compare(ldata[lidx][keyoff:keyoff+keylen], rdata[ridx][keyoff:keyoff+keylen]) < 0:
				result[i] = ldata[lidx]
				lidx++
			default:
				result[i] = rdata[ridx]
				ridx++
			}
		}
	} else {
		for i := 0; i < cap(result); i++ {
			switch {
			case lidx >= len(ldata):
				result[i] = rdata[ridx]
				ridx++
			case ridx >= len(rdata):
				result[i] = ldata[lidx]
				lidx++
			case bytes.Compare(ldata[lidx], rdata[ridx]) < 0:
				result[i] = ldata[lidx]
				lidx++
			default:
				result[i] = rdata[ridx]
				ridx++
			}
		}
	}

	return result
}

func kvbmergesort(data [][]byte, reclen, keyoff, keylen int) [][]byte {
	//log.Printf("kvmergesort data %v, reclen %v, keyoff %v, keylen %v", len(data), reclen, keyoff, keylen)
	if len(data) == 1 {
		return data
	}
	if reclen > 0 {
		if keyoff+keylen > reclen {
			log.Fatal("Mergesort key must fall within record bounds")
		}
	}

	middle := len(data) / 2

	ldata := kvbmergesort(data[:middle], reclen, keyoff, keylen)
	rdata := kvbmergesort(data[middle:], reclen, keyoff, keylen)

	return kvbmerge(ldata, rdata, reclen, keyoff, keylen)
}

// https://gist.github.com/julianshen/3940045
func gmerge[E cmp.Ordered](ldata, rdata []E) []E {
	var lidx, ridx int
	result := make([]E, len(ldata)+len(rdata))

	for i := 0; i < cap(result); i++ {
		switch {
		case lidx >= len(ldata):
			result[i] = rdata[ridx]
			ridx++
		case ridx >= len(rdata):
			result[i] = ldata[lidx]
			lidx++
		//case ldata[lidx] < rdata[ridx]:
		case cmp.Less(ldata[lidx], rdata[ridx]):
			result[i] = ldata[lidx]
			lidx++
		default:
			result[i] = rdata[ridx]
			ridx++
		}
	}

	return result
}

func gmergesort[E cmp.Ordered](data []E) []E {
	//log.Printf("gmergesort data %v", len(data))
	if len(data) == 1 {
		return data
	}

	middle := len(data) / 2

	ldata := gmergesort(data[:middle])
	rdata := gmergesort(data[middle:])

	return gmerge(ldata, rdata)
}

func gmergefunc[E any](ldata, rdata []E, cmp func(a, b E) bool) []E {
	var lidx, ridx int
	result := make([]E, len(ldata)+len(rdata))

	for i := 0; i < cap(result); i++ {
		switch {
		case lidx >= len(ldata):
			result[i] = rdata[ridx]
			ridx++
		case ridx >= len(rdata):
			result[i] = ldata[lidx]
			lidx++
		case cmp(ldata[lidx], rdata[ridx]):
			result[i] = ldata[lidx]
			lidx++
		default:
			result[i] = rdata[ridx]
			ridx++
		}
	}

	return result
}

func gmergesortfunc[E any](data []E, cmp func(a, b E) bool) []E {
	// log.Printf("gmergesortfunc data %v", len(data))
	if len(data) == 1 {
		return data
	}

	middle := len(data) / 2

	ldata := gmergesortfunc(data[:middle], cmp)
	rdata := gmergesortfunc(data[middle:], cmp)

	return gmergefunc(ldata, rdata, cmp)
}

func kvsmergesort(lns []string, reclen, keyoff, keylen int) []string {
	// log.Printf("kvsmergesort lns %v, reclen %v keyoff %v keylen %v", len(lns), reclen, keyoff, keylen)
	if reclen == 0 {
		return gmergesort(lns)
	} else {
		if keyoff+keylen > reclen {
			log.Fatal("key must fall withing record boundaries")
		}
		if keylen == 0 {
			log.Fatal("kvmergesort zero length key")
		}
		return gmergesortfunc(lns, func(a, b string) bool {
			ak := a[keyoff : keyoff+keylen]
			bk := b[keyoff : keyoff+keylen]
			return cmp.Less(ak, bk)
		})
	}
}
