package merge

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

func flreadallstrings(fp *os.File, offset int64, reclen int, iomem int64) ([]string, int64, error) {

	var lns []string

	buf, err := io.ReadAll(fp)
	if err != nil {
		log.Fatal("flreadallstrings ", err)
	}
	var r io.Reader = bytes.NewReader(buf)

	recbuf := make([]byte, reclen)
	for {
		_, err := io.ReadFull(r, recbuf)
		if err != nil {
			if err != io.EOF {
				log.Fatal("flreadallstrings ", err)
			}
			return lns, 0, nil
		}
		lns = append(lns, string(recbuf))
	}
}

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
			return flreadallstrings(fp, offset, reclen, finf.Size())
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
			return lns, offset, err
		}

		buf := make([]byte, reclen)
		if bl, err = io.ReadFull(fp, buf); err != nil {
			if err == io.EOF && bl == 0 {
				return lns, int64(0), err
			}
		}

		lns = append(lns, string(buf))
		offset += int64(reclen)

		memused += int64(reclen)

		nr++
	}

}

func vlreadallstrings(fp *os.File, offset int64, iomem int64) ([]string, int64, error) {
	var lns []string
	buf, err := io.ReadAll(fp)
	if err != nil && err != io.EOF {
		log.Fatal("vlreadallstrings ", err)
	}
	lines := strings.Split(string(buf), "\n")
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		lns = append(lns, l)
	}
	return lns, offset, nil
}

func Vlreadstrings(fp *os.File, offset int64, iomem int64) ([]string, int64, error) {

	var lns []string
	var memused int64

	if fp != os.Stdin {
		finf, err := fp.Stat()
		if err != nil {
			log.Fatal("vlreadstrings stat ", err)
		}
		if finf.Size() <= iomem {
			return vlreadallstrings(fp, offset, finf.Size())
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
			//log.Println("vlreadstrings memused >= iomem")
			return lns, offset, nil
		}

		l, err := nw.ReadString('\n')
		// Seek seens to return the buffer offset
		offset += int64(len(l))
		if err != nil {
			if err == io.EOF {
				if len(l) != 0 {
					lns = append(lns, l)
				}
				return lns, offset, err
			}
			log.Fatal("vlreadstrings readstring ", err)
		}

		lns = append(lns, l)

		memused += int64(len(l))
	}
}
