package merge

import (
	"bufio"
	"bytes"
	"container/heap"
	"log"
	"os"
)

type kvbsitem struct {
	lns                  [][]byte
	ln                   []byte
	rlen, keyoff, keylen int
	index                int
}

type KVBSPQ []*kvbsitem

func (pq KVBSPQ) Len() int { return len(pq) }

func (pq KVBSPQ) Less(i, j int) bool {
	if pq[i].keyoff != 0 || pq[i].keylen != 0 {
		ik := pq[i].ln[pq[i].keyoff : pq[i].keyoff+pq[i].keylen]
		jk := pq[j].ln[pq[j].keyoff : pq[j].keyoff+pq[j].keylen]
		//log.Print("KVBSPQ.Less keys ", ik, " ", jk)
		return bytes.Compare(ik, jk) < 0
	}
	r := bytes.Compare(pq[i].ln, pq[j].ln) < 0
	//log.Print("KVBSPQ compare ", string(pq[i].ln), " ", string(pq[j].ln))
	return r
}

func (pq KVBSPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *KVBSPQ) Push(x interface{}) {
	n := len(*pq)
	ritem := x.(*kvbsitem)
	ritem.index = n
	*pq = append(*pq, ritem)
}

func (pq *KVBSPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	ritem := old[n-1]
	ritem.index = -1 // for safety
	*pq = old[0 : n-1]
	return ritem
}

func (pq *KVBSPQ) update(ritem *kvbsitem, value []byte) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func initbpq(reclen, keyoff, keylen int, bls [][][]byte) KVBSPQ {
	pq := make(KVBSPQ, len(bls))

	nbls := len(bls)
	for i := 0; i < nbls; i++ {
		var itm kvbsitem

		itm.lns = bls[i]
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen

		itm.ln = nextbyteslice(itm)
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)

	return pq
}

func nextbyteslice(itm kvbsitem) []byte {

	if len(itm.lns) == 0 {
		var nilb []byte
		return nilb
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
// bls - slice of byte slices
func kvpqbslicesmerge(reclen, keyoff, keylen int, bls [][][]byte) [][]byte {
	log.Printf("kvpqbslicemerge %v %v %v %v", reclen, keyoff, keylen, len(bls))
	pq := initbpq(reclen, keyoff, keylen, bls)

	var oln int
	for i := range bls {
		oln += len(bls[i])
	}
	osl := make([][]byte, 0, oln)

	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvbsitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqssliceemit pop line ", string(ritem.ln))
		}
		osl = append(osl, ritem.ln)

		ritem.ln = nextbyteslice(*ritem)

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln)
	}
	return osl
}

// kvpqbsliceemit
// merge files using priority queue with records represented as byte slices
// ofp - file pointer to destination file
// reclen - record length for fixed length records
// keyoff - offset of key for fixed length record
// keylen - length of key for fixed length record
// bls    - byte slices to merge
func kvpqbsliceemit(ofp *os.File, reclen int, keyoff int, keylen int, bls [][][]byte) {

	// log.Printf("kvpqbsliceemit merging fp %v, reclen %v keyoff %v,
	// keylen %v", ofp, reclen, keyoff, keylen)
	// log.Print("kvpqbsliceemit merging %v slices", len(bls))
	pq := make(KVBSPQ, len(bls))

	lbls := len(bls)
	for i := 0; i < lbls; i++ {
		var itm kvbsitem

		itm.lns = bls[i]
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen

		itm.ln = nextbyteslice(itm)
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvbsitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqbsliceemit pop line ", string(ritem.ln))
		}
		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqbsliceemit writestring ", err)
		}

		ritem.ln = nextbyteslice(*ritem)
		if err != nil {
			continue
		}

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln)
		ne++
	}
	err := nw.Flush()
	if err != nil {
		log.Fatal("kvpqbsliceemit flush ", err)
	}
	//log.Print("kvpqbsliceemit lines written ", ne)

}
