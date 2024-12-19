package merge

import (
	"bufio"
	"container/heap"
	"io"
	"log"
	"os"
	"strings"
)

type kvsitem struct {
	fn                   string
	ln                   string
	br                   *bufio.Reader
	rlen, keyoff, keylen int
	index                int
}

type KVSPQ []*kvsitem

func (pq KVSPQ) Len() int { return len(pq) }

func (pq KVSPQ) Less(i, j int) bool {
	if pq[i].keyoff != 0 || pq[i].keylen != 0 {
		ik := pq[i].ln[pq[i].keyoff : pq[i].keyoff+pq[i].keylen]
		jk := pq[j].ln[pq[j].keyoff : pq[j].keyoff+pq[j].keylen]
		//log.Print("KVSPQ.Less keys ", ik, " ", jk)
		return strings.Compare(ik, jk) < 0
	}
	r := strings.Compare(pq[i].ln, pq[j].ln) < 0
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
	ritem := x.(*kvsitem)
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

func (pq *KVSPQ) update(ritem *kvsitem, value string) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func nextsitem(itm kvsitem) (string, error) {

	var ln string
	var err error
	if itm.rlen != 0 {
		buf := make([]byte, itm.rlen)
		n, err := io.ReadFull(itm.br, buf)
		if err != nil {
			if err == io.EOF {
				return string(buf), err
			}
			log.Fatal("kvpqread readfull ", n, " ", err)
		}
		ln = string(buf)
	} else {
		ln, err = itm.br.ReadString('\n')
		if err != nil {
			// log.Println("nextsitem readstring ", err)
			if err == io.EOF {
				return ln, err
			}
			log.Fatal("kvpqread readstring ", err)
		}
	}

	return ln, nil
}

// kvpqsreademit
// merge sorted string files using a priority queue
// ofp - output file pointer
// reclen - key lengths for fixed length records
// keyoff - offset of key in fixed length record
// keylen - length of key in fixed length record
// fns - sorted filed to merge
func kvpqsreademit(ofp *os.File, reclen int, keyoff int, keylen int, fns []string) {

	//log.Print("kvpqsreademit merging ", fns)
	pq := make(KVSPQ, len(fns))

	for i, fn := range fns {
		var itm kvsitem

		fp, err := os.Open(fn)
		if err != nil {
			log.Fatal("kvpqsreademit setup open ", fn, " ", err)
		}
		defer fp.Close()

		itm.fn = fn
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen
		rdr := io.Reader(fp)
		itm.br = bufio.NewReader(rdr)

		itm.ln, err = nextsitem(itm)
		if err != nil {
			log.Fatal("kvpqsreademit setup nextsitem ", fn, " ", err)
		}
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvsitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqsreademit pop newline ", string(ritem.ln))
		}
		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqsreademit writestring ", err)
		}

		ritem.ln, err = nextsitem(*ritem)
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
		log.Fatal("kvpqsreademit flush ", err)
	}
	//log.Print("kvpqsreademit lines written ", ne)

}
