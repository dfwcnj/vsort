package merge

import (
	"bufio"
	"bytes"
	"container/heap"
	"io"
	"log"
	"os"
	"regexp"
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

func klbchan(fn string, reclen, keyoff, keylen int, ouch chan []byte) {
	fp, e := os.Open(fn)
	if e != nil {
		log.Fatal(e)
	}
	defer fp.Close()
	defer close(ouch)
	rdr := io.Reader(fp)
	br := bufio.NewReader(rdr)

	for {
		if reclen == 0 {
			l, err := br.ReadString('\n')
			if err != nil {
				// log.Println("nextbitem readstring ", err)
				if err == io.EOF {
					ouch <- []byte(l)
					return
				}
				log.Fatal("klbchan readstring ", err)
			}
			// log.Printf("klbchan string %v:%v", fn, l)
			ouch <- []byte(l)
			// log.Print("nextbitem readstring ", l)
		} else {
			l := make([]byte, reclen)
			n, err := io.ReadFull(br, l)
			if err != nil {
				if err == io.EOF {
					ouch <- l
					return
				}
				log.Fatal("klbchan readfull ", n, " ", err)
			}
			// log.Printf("klbchan []byte %v:%v", fn, string(l))
			ouch <- l
		}
	}

}

func kvpqbreadch(ofp *os.File, reclen, keyoff, keylen int, fns []string) {
	pq := make(KBBCHQ, len(fns))

	var bre string = "[0-9A-Za-z]+"
	cre, err := regexp.Compile(bre)
	if err != nil {
		log.Fatalf("kvpqbreadch regexp \"%v\": %v", bre, err)
	}

	for i, fn := range fns {
		var ritem kvbchitem

		inch := make(chan []byte, reclen)
		go klbchan(fn, reclen, keyoff, keylen, inch)
		// log.Printf("kvpqbreadch klbchan %v", fn)

		ritem.fn = fn
		ritem.ln = <-inch
		if cre.Match(ritem.ln) == false {
			log.Fatalf("kvpqbreadch %v init failed for %v:%v", bre, ritem.fn, ritem.ln)
		}
		ritem.inch = inch
		ritem.index = i
		pq[i] = &ritem
	}

	heap.Init(&pq)

	nw := bufio.NewWriter(ofp)

	for pq.Len() > 0 {
		ritem := heap.Pop(&pq).(*kvbchitem)

		if cre.Match(ritem.ln) == false {
			// log.Printf("kvpqbreadch %v %v:%v", bre, ritem.fn, ritem.ln)
			continue
		}

		_, err = nw.WriteString(string(ritem.ln))
		if err != nil {
			log.Fatal("kvpqbreademit writestring ", err)
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
	err = nw.Flush()
	if err != nil {
		log.Fatal("kvpqbreadch flush ", err)
	}
	//log.Print("kvpqbreadch lines written ", ne)
}
