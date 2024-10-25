package merge

import (
	"bufio"
	"container/heap"
	"io"
	"log"
	"os"
	"strings"
)

// kln.key serves as the priority
type kvschitem struct {
	fn                   string
	ln                   string
	inch                 chan string
	rlen, keyoff, keylen int
	index                int
}

type KVSCHQ []*kvschitem

func (pq KVSCHQ) Len() int { return len(pq) }

func (pq KVSCHQ) Less(i, j int) bool {
	if pq[i].keyoff != 0 || pq[i].keylen != 0 {
		ik := pq[i].ln[pq[i].keyoff : pq[i].keyoff+pq[i].keylen]
		jk := pq[j].ln[pq[j].keyoff : pq[j].keyoff+pq[j].keylen]
		//log.Print("KVBPQ.Less keys ", ik, " ", jk)
		return strings.Compare(ik, jk) < 0
	}
	r := strings.Compare(pq[i].ln, pq[j].ln) < 0
	//log.Print("KVBPQ compare ", string(pq[i].ln), " ", string(pq[j].ln))
	return r
}

func (pq KVSCHQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *KVSCHQ) Push(x interface{}) {
	n := len(*pq)
	ritem := x.(*kvschitem)
	ritem.index = n
	*pq = append(*pq, ritem)
}

func (pq *KVSCHQ) Pop() interface{} {
	old := *pq
	n := len(old)
	ritem := old[n-1]
	ritem.index = -1 // for safety
	*pq = old[0 : n-1]
	return ritem
}

func (pq *KVSCHQ) update(ritem *kvschitem, value string, priority string) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func klschan(fn string, reclen, keyoff, keylen int, out chan string) {
	fp, e := os.Open(fn)
	if e != nil {
		log.Fatal(e)
	}
	defer fp.Close()
	defer close(out)
	rdr := io.Reader(fp)
	br := bufio.NewReader(rdr)

	for {
		if reclen == 0 {
			ln, err := br.ReadString('\n')
			if err != nil {
				// log.Println("klschan readstring ", err)
				if err == io.EOF {
					out <- ln
					return
				}
				log.Fatal("klschan readstring ", err)
			}
			out <- ln
			// log.Print("klschan readstring ", l)
		} else {
			l := make([]byte, reclen)
			n, err := io.ReadFull(br, l)
			if err != nil {
				if err == io.EOF {
					out <- string(l)
					return
				}
				log.Fatal("klschan readfull ", n, " ", err)
			}
			out <- string(l)
		}
	}

}

func kvpqschanemit(ofp *os.File, reclen, keyoff, keylen int, fns []string) {
	pq := make(KVSCHQ, len(fns))

	for i, fn := range fns {
		var itm kvschitem

		inch := make(chan string)
		go klschan(fn, reclen, keyoff, keylen, inch)

		itm.ln = <-inch
		itm.inch = inch
		itm.index = i
		pq[i] = &itm
	}

	heap.Init(&pq)

	nw := bufio.NewWriter(ofp)

	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvschitem)
		if string(ritem.ln) == "\n" {
			log.Fatal("kvpqsreademit pop line ", string(ritem.ln))
		}
		_, err := nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqsreademit writestring ", err)
		}

		ln, ok := <-ritem.inch
		if !ok {
			continue
		}
		ritem.ln = ln
		//ritem.rlen = reclen
		//ritem.keyoff = keyoff
		//ritem.keylen = keylen

		heap.Push(&pq, ritem)
		pq.update(ritem, ritem.ln, ritem.ln)
	}
	err := nw.Flush()
	if err != nil {
		log.Fatal("kvpqbchanemit flush ", err)
	}
	//log.Print("kvpqbchanemit lines written ", ne)
}
