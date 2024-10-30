
vsort a sort command that will eventually sort and merge<br/>
fixed length or variable length data.<br/>
If the data is fixed length, vsort does not require delimiters<br/>
the data can be represented internally as byte slices or strings<br/>

Usage of ./vsort:<b/>
  -cncnt<b/>
    	sort concurrently<b/>
  -form string<b/>
    	data form bytes or string (default "string")<b/>
  -iomem string<b/>
    	max read memory size in kb, mb or gb (default "500mb")<b/>
  -keylen int<b/>
    	length of the key if not whole line<b/>
  -keyoff int<b/>
    	offset of the key<b/>
  -md string<b/>
    	merge sirectory defaults to a directory under /tmp<b/>
  -ofn string<b/>
    	output file name otherwise stdout<b/>
  -reclen int<b/>
    	length of the fixed length record<b/>
  -stype string<b/>
    	sort type: heap, merge, radix, std (default "std")<b/>
