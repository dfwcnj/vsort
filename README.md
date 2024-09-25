
vsort a sort command that will eventually sort and merge<br/>
fixed length or variable length data.<br/>
If the data is fixed length, vsort does not require delimiters<br/>
the data can be represented internally as byte slices or strings<br/>

Usage of vsort/vsort:<br/>
  -form string<br/>
    	data form bytes or string (default "string")<br/>
  -iomem string<br/>
    	max read memory size in kb, mb or gb (default "500mb")<br/>
  -keylen int<br/>
    	length of the key if not whole line<br/>
  -keyoff int<br/>
    	offset of the key<br/>
  -md string<br/>
    	merge sirectory defaults to a directory under /tmp<br/>
  -ofn string<br/>
    	output file name otherwise stdout<br/>
  -reclen int<br/>
    	length of the fixed length record<br/>
  -stype string<br/>
    	sort type: merge, radix, std (default "std")<br/>

