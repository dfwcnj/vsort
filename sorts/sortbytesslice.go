package sorts

import (
	"log"
)

// sortflbytesslice
// sort fixed lengh records using byte slice representation
// lns - slice of byte slices
// stype - sort algorithm heap insertion merge radix std(slices.sort)
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
func sortflbytesslice(lns [][]byte, stype string, reclen, keyoff, keylen int) {

	// log.Printf("sortflbyteslice lns %v dn %v, stype %v reclen %v keyoff %v keylen %v, iomem %v ", len(lns), stype, reclen, keyoff, keylen)

	switch stype {
	case "heap":
		kvbheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvbinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		lns = kvbmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		kvrsort2a(lns, reclen, keyoff, keylen)
	case "std":
		kvslicesbsort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortflbytesslice stype ", stype)
	}

	//log.Print("sortflbytesslice sorted ", len(lns))

}

// sortvlbytesslice
// sort variable lengh records
// stype - sort algorithm heap insertion merge radix std(slices,sort
// iomem - approximate amount of memory to use for operations
func sortvlbytesslice(lns [][]byte, stype string) {
	//log.Printf("sortvlbytesslice lns %v, stype %v", len(lns), stype)

	switch stype {
	case "heap":
		kvbheapsort(lns, 0, 0, 0)
	case "insertion":
		kvbinsertionsort(lns, 0, 0, 0)
	case "merge":
		lns = kvbmergesort(lns, 0, 0, 0)
	case "radix":
		rsort2ba(lns)
	case "std":
		kvslicesbsort(lns, 0, 0, 0)
	default:
		log.Fatal("sortvlbytesslice stype ", stype)
	}

	//log.Print("sortvlbytesslice sorted ", len(lns))

}
