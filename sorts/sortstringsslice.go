package sorts

import (
	"log"
	"slices"
)

// sortflstringsslice
// sort fixed lengh records using string representation
// lns - strings slice
// stype - sort algorithm heap insertion merge radix std(slices.sort)
// reclen - record length
// keyoff - offset of key in record
// keylen - key length
func sortflstringsslice(lns []string, stype string, reclen int, keyoff int, keylen int) {

	// log.Printf("sortflstringsslice lns %v stype %v reclen %v keyoff %v keylen %v, ", len(lns), stype, reclen, keyoff, keylen)

	//log.Print("sortflstringsslice ", stype, " ", len(lns))
	switch stype {
	case "heap":
		kvsheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvsinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		lns = kvsmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		rsort2sa(lns, reclen, keyoff, keylen)
	case "std":
		kvslicesssort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortflstringsslice stype ", stype)
	}
	// log.Print("sortflstringsslice sorted ", len(lns))

}

// sortvlstringsslice
// sort variable length strings
// lns strings slice
// stype - sort algorithm heap insertion merge radix std(slices.sort)
func sortvlstringsslice(lns []string, stype string) {

	// log.Printf("sortvlstringsslice lns %v stype %v", len(lns),stype)

	switch stype {
	case "heap":
		gheapsort(lns)
	case "insertion":
		ginsertionsort(lns)
	case "merge":
		lns = gmergesort(lns)
	case "radix":
		rsort2sa(lns, 0, 0, 0)
	case "std":
		slices.Sort(lns)
	default:
		log.Fatal("sortvlstringslice stype ", stype)
	}

	//log.Print("sortvlstringslice sorted ", len(lns))

}
