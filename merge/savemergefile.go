package merge

import (
	//"bufio"

	"bufio"
	"log"
	"os"
)

// save merge file
// save key and line separated by null bute
func Savemergefile(lns [][]byte, fn string, dlim string) string {

	fp, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	log.Fatal("are you calling me??")
	return "are you even calling me??"

}

func hidethis() string {
	// just to quite the errors
	var lns [][]byte
	fp := os.Stdout
	dlim := "\n"
	fn := "fix me"

	nw := bufio.NewWriter(fp)
	//sz := (len(lns[0]) + len(dlim)) / 4
	//nw := bufio.NewWriterSize(fp, sz)

	for _, ln := range lns {

		nl := string(ln) + dlim
		//log.Print(nl)
		_, err := nw.WriteString(nl)
		//_, err := io.WriteString(fp, nl)
		if err != nil {
			log.Fatal(err)
		}
	}
	//err = nw.Flush()
	//if err != nil {
	//	log.Fatal(err)
	//}
	return fn
}
