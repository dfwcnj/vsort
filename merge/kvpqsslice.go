package merge

import (
	"bufio"
	"container/heap"
	"log"
	"os"
	"strings"
)

type kvssitem struct {
	lns                  []string
	ln                   string
	rlen, keyoff, keylen int
	index                int
}

type KVSSPQ []*kvssitem

func (pq KVSSPQ) Len() int { return len(pq) }

func (pq KVSSPQ) Less(i, j int) bool {
	if pq[i].keyoff != 0 || pq[i].keylen != 0 {
		// log.Printf("kvpqsslice Less %v %v %v %v", i, len(pq[i].ln), j, len(pq[j].ln))
		ik := pq[i].ln[pq[i].keyoff : pq[i].keyoff+pq[i].keylen]
		jk := pq[j].ln[pq[j].keyoff : pq[j].keyoff+pq[j].keylen]
		//log.Print("KVSSPQ.Less keys ", ik, " ", jk)
		return strings.Compare(ik, jk) < 0
	}
	r := strings.Compare(pq[i].ln, pq[j].ln) < 0
	//log.Print("KVSSPQ compare ", string(pq[i].ln), " ", string(pq[j].ln))
	return r
}

func (pq KVSSPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *KVSSPQ) Push(x interface{}) {
	n := len(*pq)
	ritem := x.(*kvssitem)
	ritem.index = n
	*pq = append(*pq, ritem)
}

func (pq *KVSSPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	ritem := old[n-1]
	ritem.index = -1 // for safety
	*pq = old[0 : n-1]
	return ritem
}

func (pq *KVSSPQ) update(ritem *kvssitem, value string) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func initspq(reclen, keyoff, keylen int, parts [][]string) KVSSPQ {
	// log.Print("initspq")
	pq := make(KVSSPQ, len(parts))

	for i, part := range parts {
		var itm kvssitem

		if reclen != 0 && len(part[0]) != reclen {
			log.Fatalf("kvpqsslice initspq part[%v] %v", i, len(part[0]))
		}
		// initialize the structure
		// itm.lns = parts[i]
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen

		// load the first line
		itm.ln = part[0]
		itm.lns = part[1:]
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)

	return pq
}

// kvpqsslicemerge
// merge sorted string slices using a priority queue
// reclen - key lengths for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// parts - slice of string slices
func kvpqsslicesmerge(reclen, keyoff, keylen int, parts [][]string) []string {
	// log.Printf("kvpqsslicemerge %v %v %v %v", reclen, keyoff, keylen, len(parts))
	pq := initspq(reclen, keyoff, keylen, parts)

	var oln int
	for i := range parts {
		oln += len(parts[i])
	}
	osl := make([]string, 0, oln)

	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvssitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqssliceemit initspq pop newline ", string(ritem.ln))
		}
		osl = append(osl, ritem.ln)
		if len(ritem.lns) == 0 {
			continue
		}

		ritem.ln = ritem.lns[0]
		ritem.lns = ritem.lns[1:]

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln)
	}
	return osl
}

// kvpqssliceemit
// merge sorted string slices using a priority queue
// ofp - output file pointer
// reclen - key lengths for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// parts - slice of string slices
func kvpqssliceemit(ofp *os.File, reclen int, keyoff int, keylen int, parts [][]string) {

	// log.Printf("kvpqssliceemit ofp %v reclen %v keyoff %v, keylen %v", ofp, reclen, keyoff, keylen)
	// log.Printf("kvpqssliceemit merging %v parts", len(parts))

	pq := initspq(reclen, keyoff, keylen, parts)
	// log.Printf("kvpqsslieceemit pq initiated %v", pq.Len())

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvssitem)
		if string(ritem.ln) == "\n" {
			log.Print("kvpqssliceemit pop newline ", string(ritem.ln))
		}

		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqssliceemit writestring ", err)
		}
		ne++

		if len(ritem.lns) == 0 {
			continue
		}

		ritem.ln = ritem.lns[0]
		ritem.lns = ritem.lns[1:]
		// log.Printf("kvpqssliceemit  %v after", len(ritem.lns))

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln)
	}
	err := nw.Flush()
	if err != nil {
		log.Fatal("kvpqssliceemit flush ", err)
	}
	// log.Printf("kvpqssliceemit %v written ", ne)

}
