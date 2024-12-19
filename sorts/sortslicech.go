package sorts

import (
	"log"
)

// sortbytesslicech
func sortbytesslicech(lns [][]byte, stype string, reclen, keyoff, keylen int, ouch chan [][]byte) {
	// log.Printf("sortbytesslicech %v %v lines", stype, len(lns))
	var nlns = len(lns)
	if nlns == 0 {
		log.Printf("sortbytesslicech empty slice %v", nlns)
		ouch <- lns
		return
	}
	switch stype {
	case "heap":
		kvbheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvbinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		kvbmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		if keylen > 0 {
			kvrsort2a(lns, reclen, keyoff, keylen)
		} else {
			rsort2ba(lns)
		}
	case "std":
		kvslicesbsort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortbytesslicech stype ", stype)
	}
	if nlns != len(lns) {
		log.Fatalf("sortbytesslicech %v wanted %v got %v", stype, nlns, len(lns))
	}
	// log.Printf("sortbytesslicech sending %v lines", len(lns))
	ouch <- lns
}

// sortstringsslicech
func sortstringsslicech(lns []string, stype string, reclen, keyoff, keylen int, ouch chan []string) {
	// log.Printf("sortstringsslicech %v %v lines", stype, len(lns))
	var nlns = len(lns)
	if nlns == 0 {
		log.Printf("sortstringsslicech empty slice %v", nlns)
		ouch <- lns
		return
	}
	switch stype {
	case "heap":
		kvsheapsort(lns, reclen, keyoff, keylen)
	case "insertion":
		kvsinsertionsort(lns, reclen, keyoff, keylen)
	case "merge":
		kvsmergesort(lns, reclen, keyoff, keylen)
	case "radix":
		rsort2sa(lns, reclen, keyoff, keylen)
	case "std":
		kvslicesssort(lns, reclen, keyoff, keylen)
	default:
		log.Fatal("sortstringsslicech stype ", stype)
	}
	if nlns != len(lns) {
		log.Fatalf("sortstringsslicech %v wanted %v got %v", stype, nlns, len(lns))
	}
	//log.Printf("sortstringsslicech sending %v lines", len(lns))
	ouch <- lns
}
