package merge

import (
	"bufio"
	"log"
	"os"
)

// save merge file
// save key and line separated by null bute
func Saveklmergefile(klns kvallines, fn string, dlim string) (string, int) {

	// log.Println("savemergefile len delim ", len(dlim))
	var mrlen int

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	nw := bufio.NewWriter(fp)

	var n = byte(0)

	for _, kln := range klns {

		knl := string(kln.key) + string(n) + string(kln.line) + dlim
		mrlen = len(knl)

		//_, err := fp.Write([]byte(knl))
		_, err := nw.WriteString(knl)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = nw.Flush()
	if err != nil {
		log.Fatal(err)
	}
	return fn, mrlen
}

// bufSplit(buf, reclen)
//
// split the buffer into a slice containing reclen records
func bufSplit(buf []byte, reclen int) lines {
	buflen := len(buf)
	var lns lines
	for o := 0; o < buflen; o += reclen {
		rec := buf[o : o+reclen-1]
		lns = append(lns, rec)
	}
	return lns
}

func Mergeklfiles(ofn string, reclen int, fns []string) {
	// log.Print("multi step merge not implemented")

	var err error

	ofp := os.Stdout
	if ofn != "" {
		ofp, err = os.OpenFile(ofn, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer ofp.Close()
	}

	// log.Print("mergefiles pqreademit ", reclen)
	pqreademit(ofp, reclen, klnullsplit, fns)
}
