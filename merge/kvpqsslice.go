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

func initspq(reclen, keyoff, keylen int, sls [][]string) KVSSPQ {
	pq := make(KVSSPQ, len(sls))

	nsls := len(sls)
	for i := 0; i < nsls; i++ {
		var itm kvssitem

		itm.lns = sls[i]
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen

		itm.ln = nextssitem(itm)
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)

	return pq
}

func nextssitem(itm kvssitem) string {

	if len(itm.lns) == 0 {
		return ""
	}
	ln := itm.lns[0]
	itm.lns = itm.lns[1:]

	return ln
}

// kvpqsslicemerge
// merge sorted string slices using a priority queue
// reclen - key lengths for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// sls - slice of string slices
func kvpqsslicesmerge(reclen, keyoff, keylen int, sls [][]string) []string {
	log.Printf("kvpqsslicemerge %v %v %v %v", reclen, keyoff, keylen, len(sls))
	pq := initspq(reclen, keyoff, keylen, sls)

	var oln int
	for i := range sls {
		oln += len(sls[i])
	}
	osl := make([]string, 0, oln)

	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvssitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqssliceemit pop line ", string(ritem.ln))
		}
		osl = append(osl, ritem.ln)

		ritem.ln = nextssitem(*ritem)

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
// sls - slice of string slices
func kvpqssliceemit(ofp *os.File, reclen int, keyoff int, keylen int, sls [][]string) {

	//log.Printf("kvpqssliceemit merging %v slices", len(sls))
	pq := initspq(reclen, keyoff, keylen, sls)

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvssitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqssliceemit pop line ", string(ritem.ln))
		}
		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqssliceemit writestring ", err)
		}

		ritem.ln = nextssitem(*ritem)
		if err != nil {
			continue
		}
		//ritem.rlen = reclen
		//ritem.keyoff = keyoff
		//ritem.keylen = keylen

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln)
		ne++
	}
	err := nw.Flush()
	if err != nil {
		log.Fatal("kvpqssliceemit flush ", err)
	}
	//log.Print("kvpqssliceemit lines written ", ne)

}
