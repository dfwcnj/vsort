
vsort a sort command that will eventually sort and merge<br/>
fixed length or variable length data.<br/>
If the data is fixed length, vsort does not require delimiters<br/>
the data can be represented internally as byte slices or strings<br/>

Usage of vsort/vsort:
  -form string
    	data form bytes or string (default "string")
  -iomem string
    	max read memory size in kb, mb or gb (default "500mb")
  -keylen int
    	length of the key if not whole line
  -keyoff int
    	offset of the key
  -md string
    	merge sirectory defaults to a directory under /tmp
  -ofn string
    	output file name otherwise stdout
  -reclen int
    	length of the fixed length record
  -stype string
    	sort type: merge, radix, std (default "std")

