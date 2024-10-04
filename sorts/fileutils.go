package sorts

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// filelinecount
// like wc -l count all newline delimited records in a file
// fn - name of file
// returns number of lines
func filelinecount(fn string) int64 {
	cmd := exec.Command("wc", "-l", fn)
	defer cmd.Wait()
	ofp, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal("sortfiles test filelinecount pipe", err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal("sortfiles test filelinecount start", err)
	}
	r, err := io.ReadAll(ofp)
	if err != nil {
		log.Fatal("sortfiles test filelinecount read ", err)
	}
	rsl := strings.Split(string(r), " ")
	//log.Print(rsl, " ", len(rsl), " ", rsl[len(rsl)-1])
	i, err := strconv.ParseInt(rsl[len(rsl)-2], 10, 64)
	if err != nil {
		log.Fatal("sortfiles test filelinecount parse ", err)
	}
	return i
}

// filereccount
// count number of fixed length records in a file
// records need not be delimited but if they are
// the record length must include the delimiter
// fn - name of file
// rlen ¯ record length
// returns the number of fixed length records in the file
func filereccount(fn string, rlen int) int64 {
	fp, err := os.Open(fn)
	if err != nil {
		log.Fatal("filereccount ", err)
	}
	finf, err := fp.Stat()
	if err != nil {
		log.Fatal("filereccount ", err)
	}
	return finf.Size() / int64(rlen)
}

// Flfileemit
// for a fixed length record file without delimiters
// print each record on a separate line
// fn - name of file
// rlen - record length
func Flfileemit(fn string, rlen int) {
	fp, err := os.Open(fn)
	if err != nil {
		log.Fatal(fn, " ", err)
	}
	defer fp.Close()
	nr := bufio.NewReader(fp)
	r := make([]byte, 0, rlen)
	for {
		_, err := nr.Read(r)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatal("flfileemit read ", err)
		}
		fmt.Println(string(r))
	}
}
