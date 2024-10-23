package merge

import (
	"bufio"
	"container/heap"
	"fmt"
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
			// log.Print("klschan readstring ", l)
		} else {
			ln := make([]byte, reclen)
			n, err := io.ReadFull(br, ln)
			if err != nil {
				if err == io.EOF {
					out <- string(ln)
					return
				}
				log.Fatal("klschan readfull ", n, " ", err)
			}
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
		itm := heap.Pop(&pq).(*kvschitem)
		s := fmt.Sprintf("%s\n", string(itm.ln))
		_, err := nw.WriteString(s)
		if err != nil {
			log.Fatal(err)
		}

		ln, ok := <-itm.inch
		if !ok {
			continue
		}
		itm.ln = ln
		heap.Push(&pq, itm)
		pq.update(itm, itm.ln, itm.ln)
	}
	err := nw.Flush()
	if err != nil {
		log.Fatal("kvpqbchanemit flush ", err)
	}
	//log.Print("kvpqbchanemit lines written ", ne)
}
