package merge

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"log"
	"os"
)

type kvritem struct {
	ln                   []byte
	br                   *bufio.Reader
	rlen, keyoff, keylen int
	index                int
}

type KVSPQ []*kvritem

func (pq KVSPQ) Len() int { return len(pq) }

func (pq KVSPQ) Less(i, j int) bool {
	if pq[i].keyoff != 0 || pq[i].keylen != 0 {
		ik := pq[i].ln[pq[i].keyoff : pq[i].keyoff+pq[i].keylen]
		jk := pq[j].ln[pq[j].keyoff : pq[j].keyoff+pq[j].keylen]
		//log.Print("KVSPQ.Less keys ", ik, " ", jk)
		return bytes.Compare(ik, jk) < 0
	}
	r := bytes.Compare(pq[i].ln, pq[j].ln) < 0
	//log.Print("KVSPQ compare ", string(pq[i].ln), " ", string(pq[j].ln))
	return r
}

func (pq KVSPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *KVSPQ) Push(x interface{}) {
	n := len(*pq)
	ritem := x.(*kvritem)
	ritem.index = n
	*pq = append(*pq, ritem)
}

func (pq *KVSPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	ritem := old[n-1]
	ritem.index = -1 // for safety
	*pq = old[0 : n-1]
	return ritem
}

func (pq *KVSPQ) update(ritem *kvritem, value []byte) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func nextitem(itm kvritem) ([]byte, error) {

	var ln []byte

	if itm.rlen == 0 {
		l, err := itm.br.ReadString('\n')
		if err != nil {
			// log.Println("nextitem readstring ", err)
			if err == io.EOF {
				return []byte(l), err
			}
			log.Fatal("kvpqread readstring ", err)
		}
		// log.Print("nextitem readstring ", l)
		ln = []byte(l)
	} else {
		ln = make([]byte, itm.rlen)
		_, err := io.ReadFull(itm.br, ln)
		if err != nil {
			// log.Println("nextitem readfull ", err)
			if err == io.EOF {
				return ln, err
			}
			log.Fatal("kvpqread readfull ", err)
		}
	}

	return ln, nil
}

func kvpqreademit(ofp *os.File, reclen int, keyoff int, keylen int, fns []string) {

	pq := make(KVSPQ, len(fns))

	var fp *os.File
	for i, fn := range fns {
		var itm kvritem

		fp, err := os.Open(fn)
		if err != nil {
			log.Fatal("kvpqreademit setup open ", fn, " ", err)
		}

		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen
		rdr := io.Reader(fp)
		itm.br = bufio.NewReader(rdr)

		itm.ln, err = nextitem(itm)
		if err != nil {
			log.Fatal("kvpqreademit setup nextitem ", fn, " ", err)
		}
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)
	defer fp.Close()

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvritem)
		s := fmt.Sprintf("%s", string(ritem.ln))
		_, err := nw.WriteString(s)
		if err != nil {
			log.Fatal("kvpqreademit writestring ", err)
		}

		ritem.ln, err = nextitem(*ritem)
		if err != nil {
			continue
		}
		ritem.rlen = reclen
		ritem.keyoff = keyoff
		ritem.keylen = keylen

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln)
		ne++
	}
	err := nw.Flush()
	if err != nil {
		log.Fatal("kvpqreademit flush ", err)
	}
	log.Print("kvpqreademit lines written ", ne)

}
