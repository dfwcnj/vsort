package main

import (
	"flag"
	"log"
	"strconv"

	"github.com/dfwcnj/govbinsort/sorts"
)

func parseiomem(iomem string) int64 {

	ns := iomem[0 : len(iomem)-2]
	n, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	ms := iomem[len(iomem)-2:]
	switch ms {
	case "kb":
		return n * 1 << 10
	case "mb":
		return n * 1 << 20
	case "gb":
		return n * 1 << 30
	default:
		log.Fatal("bad iomem argument: ", iomem)
	}
	return 0
}

func main() {
	var fns []string
	var ofn, iomem, md, stype string
	var reclen, keylen, keyoff int
	flag.StringVar(&ofn, "ofn", "", "output file name")
	flag.StringVar(&iomem, "iomem", "500mb", "max read memory size in kb, mb or gb")
	flag.StringVar(&md, "md", "", "merge sirectory")
	flag.StringVar(&stype, "stype", "std", "sort type: merge, radix, std")
	flag.IntVar(&reclen, "reclen", 0, "length of the fixed length record")
	flag.IntVar(&keyoff, "keyoff", 0, "offset of the key")
	flag.IntVar(&keylen, "keylen", 0, "length of the key if not whole line")
	flag.Parse()
	fns = flag.Args()

	sortt := map[string]bool{
		"heap":      true,
		"insertion": true,
		"merge":     true,
		"radix":     true,
		"std":       true,
	}
	if _, ok := sortt[stype]; ok {
		log.Fatal("bad sort type ", stype)
	}
	if keyoff != 0 || keylen != 0 {
		if reclen == 0 {
			log.Fatal("keyoff, keylen only allowed in fixed len resords")
		}
		if keyoff+keylen > reclen {
			log.Fatal("key must fall within record boundaries")
		}
		if keyoff < 0 || keylen < 0 {
			log.Fatal("bad key boundaries")
		}
	}

	var iom int64
	if iomem != "" {
		iom = parseiomem(iomem)
	}
	sorts.Sortfiles(fns, ofn, md, stype, reclen, keyoff, keylen, iom)
}
