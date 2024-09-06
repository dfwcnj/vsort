package sorts

import "cmp"

// CLRS
// parent(i)
// i/2 or i>>1
// left(i)
// 2i or 1<<1
// right(i)
// 2i+1 or i<<1+1

func minheapify[S ~[]E, E cmp.Ordered](data S, i int) {
	var smallest int
	l := i << 1
	r := l + 1
	if l <= len(data) && data[l] < data[i] {
		smallest = l
	} else {
		smallest = i
	}
	if r <= len(data) && data[r] < data[smallest] {
		smallest = r
	}
	if smallest != i {
		data[i], data[smallest] = data[smallest], data[i]
		minheapify(data, smallest)
	}
}

func maxheapify[S ~[]E, E cmp.Ordered](data S, i int) {
	var largest int
	l := i << 1
	r := l + 1
	if l <= len(data) && data[l] > data[i] {
		largest = l
	} else {
		largest = i
	}
	if r <= len(data) && data[r] > data[largest] {
		largest = r
	}
	if largest != i {
		data[i], data[largest] = data[largest], data[i]
		maxheapify(data, largest)
	}
}

// sedgewick
func swim[S ~[]E, E cmp.Ordered](data S, i int) {
	for i > 0 && data[i<<1] < data[i] {
		data[i<<1], data[i] = data[i], data[i<<1]
		i = i << 1
	}

}

func ink[S ~[]E, E cmp.Ordered](data S, i int) {
	for i<<1 <= len(data) {
		j := i << 1
		if j < len(data) && data[j] < data[j+1] {
			j++
		}
		if data[i] < data[j] {
			break
		}
		data[i], data[j] = data[j], data[i]
		i = j
	}
}

func Heapsort[S ~[]E, E cmp.Ordered](data S) []E {
	if len(data) == 1 {
		return data
	}
	n := len(data)
	for i := n / 2; i > 0; i-- {
		minheapify(data, i)
	}
	return data
}
