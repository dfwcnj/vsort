package merge

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

// flreadallstrings
// read all fixed length strings from a 'small' file
// return a slice of strings, 0, and error
func flreadallstrings(fp *os.File, reclen int) ([]string, int64, error) {

	var lns []string

	buf, err := io.ReadAll(fp)
	if err != nil {
		log.Fatal("flreadallstrings ", err)
	}
	// log.Printf("flreadallstrings %v %v", reclen, len(buf))
	var r io.Reader = bytes.NewReader(buf)

	var off int64
	recbuf := make([]byte, reclen)
	for {
		n, err := io.ReadFull(r, recbuf)
		if err != nil {
			if err == io.EOF {
				if n == reclen {
					lns = append(lns, string(recbuf))
				}
			} else {
				log.Fatalf("flreadallstrings wanted %v got %v %v", reclen, n, err)
			}
			// log.Printf("flreadallstrings %v lns %v offset", len(lns), off)
			return lns, off, nil
		}
		lns = append(lns, string(recbuf))
		off += int64(n)
	}
}

// Flreadstrings
// read fixed length strings with or without delimiters
// fp - file pointer
// offset - offset into file
// reclen = record length
// iomem - amount of memory to comsume for this invocation
// return slice of strings, offset, error
func Flreadstrings(fp *os.File, offset int64, reclen int, iomem int64) ([]string, int64, error) {

	var lns []string
	var nr int // number records read
	var bl int
	var err error
	var memused int64

	if fp != os.Stdin {
		finf, err := fp.Stat()
		if err != nil {
			log.Fatal("flreadn stat ", err)
		}
		if finf.Size() <= iomem {
			return flreadallstrings(fp, reclen)
		}

		if offset != 0 {
			_, err := fp.Seek(offset, 0)
			if err != nil {
				log.Fatal("flreadn seek", err)
			}
		}
	}

	for {

		if fp != os.Stdin && memused >= iomem {
			// log.Printf("flreadallstrings iomem %v lns %v offset", len(lns), offset)
			return lns, offset, err
		}

		buf := make([]byte, reclen)
		if bl, err = io.ReadFull(fp, buf); err != nil {
			if err == io.EOF {
				// log.Printf("flreadallstrings EOF %v lns %v offset", len(lns), offset)
				if bl == reclen {
					lns = append(lns, string(buf))
				}
				return lns, offset, err
			}
			log.Fatalf("flreadallstrings %v", err)
		}

		lns = append(lns, string(buf))
		offset += int64(reclen)

		memused += int64(reclen)

		nr++
	}

}

// vlreadallstrings
// read all variable length records from a  'small' file
// return slice of strings, 0, error
func vlreadallstrings(fp *os.File) ([]string, int64, error) {
	var lns []string
	buf, err := io.ReadAll(fp)
	if err != nil && err != io.EOF {
		log.Fatal("vlreadallstrings ", err)
	}
	// log.Printf("vlreadallstrings read %v", len(buf))

	lines := strings.Split(string(buf), "\n")
	var off int64
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		lns = append(lns, l)
		off += int64(len(l))
	}
	// log.Printf("vlreadallstrings %v lns %v offset", len(lns), off)
	return lns, off, nil
}

// Vlreadstrings
// read variable length strings from a file
// fp - file pointer
// offset - offset into file
// iomem - amount of memory to consume reading the records
func Vlreadstrings(fp *os.File, offset int64, iomem int64) ([]string, int64, error) {

	var lns []string
	var memused int64

	if fp != os.Stdin {
		finf, err := fp.Stat()
		if err != nil {
			log.Fatal("vlreadstrings stat ", err)
		}
		if finf.Size() <= iomem {
			return vlreadallstrings(fp)
		}

		if offset != 0 {
			_, err := fp.Seek(offset, 0)
			if err != nil {
				log.Fatal("vlreadstrings seek", err)
			}
		}
	}

	r := io.Reader(fp)
	nw := bufio.NewReader(r)

	for {
		if fp != os.Stdin && memused >= iomem {
			// log.Printf("vlreadstrings iomem %v lns %v offset", len(lns), offset)
			return lns, offset, nil
		}

		l, err := nw.ReadString('\n')
		if !strings.HasSuffix(l, "\n") {
			// contrary to pkg.go.dev/bufio@go1.23.4#Reader.ReadString
			l = l + "\n"
		}
		// Seek seens to return the buffer offset
		offset += int64(len(l))
		if err != nil {
			if err == io.EOF {
				if len(l) != 0 {
					lns = append(lns, l)
				}
				// log.Printf("vlreadstrings EOF  %v lns %v offset", len(lns), offset)
				return lns, offset, err
			}
			log.Fatal("vlreadstrings readstring ", err)
		}

		lns = append(lns, l)

		memused += int64(len(l))
	}
}
