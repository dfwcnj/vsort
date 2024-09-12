package sorts

import (
	"io"
	"log"
	"os/exec"
	"strconv"
)

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
	i, err := strconv.ParseInt(string(r), 10, 64)
	if err != nil {
		log.Fatal("sortfiles test filelinecount parse ", err)
	}
	return i
}
