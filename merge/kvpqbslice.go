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

func initbpq(reclen, keyoff, keylen int, bparts [][][]byte) KVBSPQ {
	log.Print("initbpq")
	pq := make(KVBSPQ, len(bparts))

	nbparts := len(bparts)
	for i := 0; i < nbparts; i++ {
		var itm kvbsitem

		itm.lns = bparts[i]
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen

		itm.ln = itm.lns[0]
		itm.lns = itm.lns[1:]
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
// bparts - slice of byte slices
func kvpqbslicesmerge(reclen, keyoff, keylen int, bparts [][][]byte) [][]byte {
	// log.Printf("kvpqbslicemerge %v %v %v %v", reclen, keyoff, keylen, len(bparts))
	pq := initbpq(reclen, keyoff, keylen, bparts)

	var oln int
	for i := range bparts {
		oln += len(bparts[i])
	}
	osl := make([][]byte, 0, oln)

	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvbsitem)
		if len(ritem.lns) == 0 {
			continue
		}
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqssliceemit pop line ", string(ritem.ln))
		}
		osl = append(osl, ritem.ln)

		ritem.ln = ritem.lns[0]
		ritem.lns = ritem.lns[1:]

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
// bparts    - byte slices to merge
func kvpqbsliceemit(ofp *os.File, reclen int, keyoff int, keylen int, bparts [][][]byte) {

	// log.Printf("kvpqbsliceemit merging fp %v, reclen %v keyoff %v, keylen %v", ofp, reclen, keyoff, keylen)
	// log.Printf("kvpqbsliceemit merging %v parts", len(bparts))

	pq := initbpq(reclen, keyoff, keylen, bparts)
	// log.Printf("kvpqbsliceemit pq initiated %v", pq.Len())

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvbsitem)
		if len(ritem.lns) == 0 {
			continue
		}
		// log.Printf("kvpqbsliceemit line pop  %v", string(ritem.ln))
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqbsliceemit pop line ", string(ritem.ln))
		}
		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqbsliceemit writestring ", err)
		}

		// log.Printf("kvpqbsliceemit %v slices before", len(ritem.lns))
		ritem.ln = ritem.lns[0]
		ritem.lns = ritem.lns[1:]
		// log.Printf("kvpqbsliceemit %v slices after", len(ritem.lns))

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
