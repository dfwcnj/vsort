package merge

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

// flreadallbytes
// read fixed length lines from a 'small' file
// return slice of byte slices, 0, error
func flreadallbytes(fp *os.File, reclen int, iomem int64) ([][]byte, int64, error) {

	var lns [][]byte

	buf, err := io.ReadAll(fp)
	if err != nil {
		log.Fatal("flreadall ", err)
	}
	var r io.Reader = bytes.NewReader(buf)

	var off int64
	recbuf := make([]byte, reclen)
	for {
		_, err := io.ReadFull(r, recbuf)
		if err != nil {
			if err != io.EOF {
				log.Fatal("flreadall ", err)
			}
			return lns, off, nil
		}
		lns = append(lns, recbuf)
		off += int64(reclen)
	}
}

// Flreadbytes
// fp - file pointer
// offset - offset into file
// reclen - length of each fixed length record
// iomem  - amount of memory to use for each invocation
// return slice of byte slices, offset, error
func Flreadbytes(fp *os.File, offset int64, reclen int, iomem int64) ([][]byte, int64, error) {

	var lns [][]byte
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
			return flreadallbytes(fp, reclen, finf.Size())
		}

		if offset != 0 {
			_, err := fp.Seek(offset, 0)
			if err != nil {
				log.Fatal("flreadn seek", err)
			}
		}
		// log.Printf("Flreadbytes fn %v offset %v reclen %v", finf.Name(), offset, reclen)
	}

	for {

		if fp != os.Stdin && memused >= iomem {
			// log.Print("Flreadbytes ", len(lns), " ", offset)
			return lns, offset, err
		}

		buf := make([]byte, reclen)
		if bl, err = io.ReadFull(fp, buf); err != nil {
			if err == io.EOF && bl == 0 {
				// log.Print("Flreadbytes ", len(lns))
				return lns, offset, err
			}
		}

		lns = append(lns, buf)
		offset += int64(reclen)

		memused += int64(reclen)

		nr++
	}

}

// vlreadallbytes
// read all variable length newline delimited records from a 'small' file
// return a slice of byte slices, 0, and error
func vlreadallbytes(fp *os.File, iomem int64) ([][]byte, int64, error) {
	var lns [][]byte
	buf, err := io.ReadAll(fp)
	if err != nil && err != io.EOF {
		log.Fatal("vlreadall ", err)
	}
	lines := strings.Split(string(buf), "\n")
	var off int64
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		bln := []byte(l)
		lns = append(lns, bln)
		off += int64(len(l))
	}
	return lns, off, nil
}

// vlreadbytes
// fp file pointer
// offset - offset into file
// iomem - amount of memory to spend for each invocation
// XXX for now, the delimiter is assumed to be \n
// return a slice of byte slices, offset, anr error
func Vlreadbytes(fp *os.File, offset int64, iomem int64) ([][]byte, int64, error) {

	var lns [][]byte
	var memused int64

	if fp != os.Stdin {
		finf, err := fp.Stat()
		if err != nil {
			log.Fatal("vlreadbytes stat ", err)
		}
		if finf.Size() <= iomem {
			return vlreadallbytes(fp, finf.Size())
		}

		if offset != 0 {
			_, err := fp.Seek(offset, 0)
			if err != nil {
				log.Fatal("vlreadbytes seek", err)
			}
		}
	}

	r := io.Reader(fp)
	nw := bufio.NewReader(r)

	for {
		if fp != os.Stdin && memused >= iomem {
			//log.Println("vlreadbytes memused >= iomem")
			return lns, offset, nil
		}

		l, err := nw.ReadString('\n')
		// Seek seens to return the buffer offset
		offset += int64(len(l))
		if err != nil {
			if err == io.EOF {
				if len(l) != 0 {
					lns = append(lns, []byte(l))
				}
				return lns, offset, err
			}
			log.Fatal("vlreadbytes readstring ", err)
		}

		bln := []byte(l)

		lns = append(lns, bln)

		memused += int64(len(l))
	}
}
