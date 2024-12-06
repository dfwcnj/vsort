package sorts

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

// kvbmergesort
// sort fixed length records represented as byte slices with merge sort
// data - slice of byte slices
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
// returns a slice of sorted byte slices
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

	tdata := kvbmerge(ldata, rdata, reclen, keyoff, keylen)
	copy(data, tdata)
	return data
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

// gmergesort
// sort cmp.Ordered compatible data with merge sort
// data - slice of cmp.Ordered compatible data
// returns - sorted slice of cmp.Ordered compatible data
func gmergesort[E cmp.Ordered](data []E) []E {
	//log.Printf("gmergesort data %v", len(data))
	if len(data) == 1 {
		return data
	}

	middle := len(data) / 2

	ldata := gmergesort(data[:middle])
	rdata := gmergesort(data[middle:])

	tdata := gmerge(ldata, rdata)
	copy(data, tdata)
	return data
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

// gmergesortfunc
// supposedly generic mergesort but only works with strings
// data - slice of data
// func - comparison function
// returns slice of sorted data
func gmergesortfunc[E any](data []E, cmp func(a, b E) bool) []E {
	if len(data) == 0 {
		return data
	}
	// log.Printf("gmergesortfunc data %v", len(data))
	if len(data) == 1 {
		return data
	}

	middle := len(data) / 2

	ldata := gmergesortfunc(data[:middle], cmp)
	rdata := gmergesortfunc(data[middle:], cmp)

	tdata := gmergefunc(ldata, rdata, cmp)
	copy(data, tdata)
	return data
}

// kvsmergesort
// sort fixed length strings with mergesort
// lns - slice of strings
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
// returns sorted slice of strings
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
