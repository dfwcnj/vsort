package sorts

import (
	"cmp"
)

func Insertionsort[S ~[]E, E cmp.Ordered](data S) []E {
	n := len(data)
	if n == 1 {
		return data
	}
	for i := 0; i < n; i++ {
		for j := i; j > 0 && data[j-1] > data[j]; j-- {
			data[j], data[j-1] = data[j-1], data[j]
		}
	}
	return data
}
