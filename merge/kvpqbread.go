package merge

import (
	"bufio"
	"bytes"
	"container/heap"
	"io"
	"log"
	"os"
)

type kvbitem struct {
	fn                   string
	ln                   []byte
	br                   *bufio.Reader
	rlen, keyoff, keylen int
	index                int
}

type KVBPQ []*kvbitem

func (pq KVBPQ) Len() int { return len(pq) }

func (pq KVBPQ) Less(i, j int) bool {
	if pq[i].keyoff != 0 || pq[i].keylen != 0 {
		ik := pq[i].ln[pq[i].keyoff : pq[i].keyoff+pq[i].keylen]
		jk := pq[j].ln[pq[j].keyoff : pq[j].keyoff+pq[j].keylen]
		//log.Print("KVBPQ.Less keys ", ik, " ", jk)
		return bytes.Compare(ik, jk) < 0
	}
	r := bytes.Compare(pq[i].ln, pq[j].ln) < 0
	//log.Print("KVBPQ compare ", string(pq[i].ln), " ", string(pq[j].ln))
	return r
}

func (pq KVBPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *KVBPQ) Push(x interface{}) {
	n := len(*pq)
	ritem := x.(*kvbitem)
	ritem.index = n
	*pq = append(*pq, ritem)
}

func (pq *KVBPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	ritem := old[n-1]
	ritem.index = -1 // for safety
	*pq = old[0 : n-1]
	return ritem
}

func (pq *KVBPQ) update(ritem *kvbitem, value []byte) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func nextbitem(itm kvbitem) ([]byte, error) {

	var ln []byte

	if itm.rlen == 0 {
		l, err := itm.br.ReadString('\n')
		if err != nil {
			// log.Println("nextbitem readstring ", err)
			if err == io.EOF {
				return []byte(l), err
			}
			log.Fatal("kvpqread readstring ", err)
		}
		// log.Print("nextbitem readstring ", l)
		ln = []byte(l)
	} else {
		ln = make([]byte, itm.rlen)
		n, err := io.ReadFull(itm.br, ln)
		if err != nil {
			if err == io.EOF {
				return ln, err
			}
			log.Fatal("kvpqread readfull ", n, " ", err)
		}
	}

	return ln, nil
}

func kvpqbreademit(ofp *os.File, reclen int, keyoff int, keylen int, fns []string) {

	finf, err := ofp.Stat()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("kvpqbreademit merging fn %s, reclen %d keyoff %d, keylen %d", finf.Name(), reclen, keyoff, keylen)
	log.Print("kvpqbreademit merging ", fns)
	pq := make(KVBPQ, len(fns))

	for i, fn := range fns {
		var itm kvbitem

		fp, err := os.Open(fn)
		if err != nil {
			log.Fatal("kvpqbreademit setup open ", fn, " ", err)
		}
		defer fp.Close()

		itm.fn = fn
		itm.rlen = reclen
		itm.keyoff = keyoff
		itm.keylen = keylen
		rdr := io.Reader(fp)
		itm.br = bufio.NewReader(rdr)

		itm.ln, err = nextbitem(itm)
		if err != nil {
			log.Fatal("kvpqbreademit setup nextbitem ", fn, " ", err)
		}
		itm.index = i

		pq[i] = &itm
	}

	heap.Init(&pq)

	nw := bufio.NewWriter(ofp)
	defer nw.Flush()

	var ne int64
	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvbitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqbreademit pop line ", string(ritem.ln))
		}
		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqbreademit writestring ", err)
		}

		ritem.ln, err = nextbitem(*ritem)
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
	err = nw.Flush()
	if err != nil {
		log.Fatal("kvpqbreademit flush ", err)
	}
	//log.Print("kvpqbreademit lines written ", ne)

}
