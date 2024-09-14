package merge

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

func flreadall(fp *os.File, offset int64, reclen int, iomem int64) ([][]byte, int64, error) {

	var lns [][]byte

	buf, err := io.ReadAll(fp)
	if err != nil {
		log.Fatal("flreadall ", err)
	}
	var r io.Reader = bytes.NewReader(buf)

	recbuf := make([]byte, reclen)
	for {
		_, err := io.ReadFull(r, recbuf)
		if err != nil {
			if err != io.EOF {
				log.Fatal("flreadall ", err)
			}
			return lns, 0, nil
		}
		lns = append(lns, recbuf)
	}
}

func Flreadn(fp *os.File, offset int64, reclen int, iomem int64) ([][]byte, int64, error) {

	var lns [][]byte
	var nr int // number records read
	var bl int
	var err error
	var memused int64

	finf, err := fp.Stat()
	if err != nil {
		log.Fatal("flreadn stat ", err)
	}
	if finf.Size() <= iomem {
		return flreadall(fp, offset, reclen, finf.Size())
	}

	if offset != 0 {
		if fp.Name() == "/dev/stdin" {
			log.Fatal("flreadn(stdin) more than iomem")
		}
		//log.Printf("flreadn %s  seeking to %d\n", fp.Name(), offset)
		o, err := fp.Seek(offset, 0)
		if err != nil {
			log.Fatal("flreadn seek", err)
		}
		if o != offset {
			log.Fatal("flreadn seek wanted", offset, " got ", o)
		}
	}
	for {

		if memused >= iomem {
			offset, err = fp.Seek(0, 1)
			if err != nil && err != io.EOF {
				log.Fatal("flreadn seek ", err)
			}
			return lns, offset, err
		}

		buf := make([]byte, reclen)
		if bl, err = io.ReadFull(fp, buf); err != nil {
			if err == io.EOF && bl == 0 {
				return lns, int64(0), err
			}
		}

		lns = append(lns, buf)

		memused += int64(reclen)

		nr++
	}

}

func vlreadall(fp *os.File, offset int64, iomem int64) ([][]byte, int64, error) {
	var lns [][]byte
	buf, err := io.ReadAll(fp)
	if err != nil && err != io.EOF {
		log.Fatal("vlreadall ", err)
	}
	lines := strings.Split(string(buf), "\n")
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		bln := []byte(l)
		lns = append(lns, bln)
	}
	return lns, offset, nil
}

func Vlreadn(fp *os.File, offset int64, iomem int64) ([][]byte, int64, error) {

	var lns [][]byte
	var memused int64

	finf, err := fp.Stat()
	if err != nil {
		log.Fatal("vlreadn stat ", err)
	}
	if finf.Size() <= iomem {
		return vlreadall(fp, offset, finf.Size())
	}

	if offset != 0 {
		if fp.Name() == "/dev/stdin" {
			log.Fatal("vlreadn(stdin) offset ", offset)
		}
		//log.Printf("vlreadn %s  seeking to %d\n", fp.Name(), offset)
		fp.Seek(offset, 0)
	}

	r := io.Reader(fp)
	nw := bufio.NewReader(r)

	for {
		if memused >= iomem {
			//log.Println("vlreadn memused >= iomem")
			return lns, offset, err
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
			log.Fatal("vlreadn readstring ", err)
		}

		bln := []byte(l)

		lns = append(lns, bln)

		memused += int64(len(l))
	}
}
