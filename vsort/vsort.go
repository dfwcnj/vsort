package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/dfwcnj/vsort/sorts"
)

// parseiomem convert iomem string to an io memory size
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

var CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
var usage = func() {
	fmt.Fprintf(CommandLine.Output(), "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

// main - sort command
// main args files
func main() {
	var fns []string
	var ofn, iomem, md, stype, form string
	var reclen, keylen, keyoff int
	var cncnt bool

	flag.StringVar(&ofn, "ofn", "", "output file name otherwise stdout")
	flag.StringVar(&iomem, "iomem", "500mb", "max read memory size in kb, mb or gb")
	flag.StringVar(&md, "md", "", "merge sirectory defaults to a directory under /tmp")
	flag.StringVar(&stype, "stype", "std", "sort type: heap, merge, radix, std")
	flag.StringVar(&form, "form", "string", "data form bytes or string")
	flag.IntVar(&reclen, "reclen", 0, "length of the fixed length record")
	flag.IntVar(&keyoff, "keyoff", 0, "offset of the key")
	flag.IntVar(&keylen, "keylen", 0, "length of the key if not whole line")
	flag.BoolVar(&cncnt, "cncnt", false, "sort concurrently")

	flag.Parse()
	fns = flag.Args()
	if len(fns) == 0 {
		cncnt = false
	}

	sortt := map[string]bool{
		"heap":      true,
		"insertion": true,
		"merge":     true,
		"radix":     true,
		"std":       true,
	}
	if _, ok := sortt[stype]; !ok {
		log.Print("bad sort type ", stype)
		usage()
	}
	if form != "string" && form != "bytes" {
		log.Print("bad form type ", stype)
		usage()
	}
	if reclen != 0 && keylen == 0 {
		keylen = reclen - keyoff
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
	log.Printf("flags: ofn %v, md %v, stype %v, reclen %v, keyoff %v, keylen %v, iom %v", ofn, md, stype, reclen, keyoff, keylen, iom)
	log.Printf("args %v", fns)
	if form == "bytes" {
		if cncnt == false {
			sorts.Sortbytesfiles(fns, ofn, md, stype, reclen, keyoff, keylen, iom)
		} else {
			sorts.Sortbytesfilesch(fns, ofn, md, stype, reclen, keyoff, keylen, iom)
		}

	} else {
		if cncnt == false {
			sorts.Sortstringsfiles(fns, ofn, md, stype, reclen, keyoff, keylen, iom)
		} else {
			sorts.Sortstringsfilesch(fns, ofn, md, stype, reclen, keyoff, keylen, iom)
		}
	}

}
