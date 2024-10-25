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

// kln.key serves as the priority
type kvbchitem struct {
	fn                   string
	ln                   []byte
	inch                 chan []byte
	rlen, keyoff, keylen int
	index                int
}

type KBBCHQ []*kvbchitem

func (pq KBBCHQ) Len() int { return len(pq) }

func (pq KBBCHQ) Less(i, j int) bool {
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

func (pq KBBCHQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *KBBCHQ) Push(x interface{}) {
	n := len(*pq)
	ritem := x.(*kvbchitem)
	ritem.index = n
	*pq = append(*pq, ritem)
}

func (pq *KBBCHQ) Pop() interface{} {
	old := *pq
	n := len(old)
	ritem := old[n-1]
	ritem.index = -1 // for safety
	*pq = old[0 : n-1]
	return ritem
}

func (pq *KBBCHQ) update(ritem *kvbchitem, value []byte, priority []byte) {
	ritem.ln = value
	heap.Fix(pq, ritem.index)
}

func klchan(fn string, reclen, keyoff, keylen int, out chan []byte) {
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
			var ln []byte
			l, err := br.ReadString('\n')
			if err != nil {
				// log.Println("nextbitem readstring ", err)
				ln := []byte(l)
				if err == io.EOF {
					out <- ln
					return
				}
				log.Fatal("klchan readstring ", err)
			}
			out <- ln
			// log.Print("nextbitem readstring ", l)
		} else {
			ln := make([]byte, reclen)
			n, err := io.ReadFull(br, ln)
			if err != nil {
				if err == io.EOF {
					out <- ln
					return
				}
				log.Fatal("klchan readfull ", n, " ", err)
			}
			out <- ln
		}
	}

}

func kvpqbchanemit(ofp *os.File, reclen, keyoff, keylen int, fns []string) {
	pq := make(KBBCHQ, len(fns))

	for i, fn := range fns {
		var itm kvbchitem

		inch := make(chan []byte, reclen)
		go klchan(fn, reclen, keyoff, keylen, inch)

		itm.ln = <-inch
		itm.inch = inch
		itm.index = i
		pq[i] = &itm
	}

	heap.Init(&pq)

	nw := bufio.NewWriter(ofp)

	for pq.Len() > 0 {
		itm := heap.Pop(&pq).(*kvbchitem)
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
