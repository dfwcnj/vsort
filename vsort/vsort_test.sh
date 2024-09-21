#! /bin/sh
set -ex

# I use https://github.com/dfwcnj/goranddatagen to generate random strings
# for this
# Usage of goranddatagen:
#   -datatype string
#     	type of data to sort - string, uint64, datetime (default "string")
#   -format string
#     	if datatype is datetime, what date format (default "RFC3339")
#   -len int
#     	max length for random strings (default 32)
#   -n int
#     	number of random data elements to generate (default 1048576)
#   -nl
#     	emit strings with newlines
#   -rlen
#     	random lengths
rm -r /tmp/[fmSs]*
goranddatagen -n 16777216  >/tmp/bdata0
goranddatagen -n 16777216  >/tmp/bdata1
goranddatagen -n 33554432 -rlen >/tmp/rdata0
goranddatagen -n 33554432 -rlen >/tmp/rdata1
# https://github.com/dfwcnj/flcat
# Usage of flcat:
#   -fn string
#     	name of fl file to emit
#   -klen int
#     	record key length
#   -koff int
#     	offset of key in record
#   -rlen int
#     	record length
# https://github.com/dfwcnj/govbinsort
# Usage of vsort:
#   -form string
#     	data form bytes or string (default "string")
#   -iomem string
#     	max read memory size in kb, mb or gb (default "500mb")
#   -keylen int
#     	length of the key if not whole line
#   -keyoff int
#     	offset of the key
#   -md string
#     	merge sirectory
#   -ofn string
#     	output file name
#   -reclen int
#     	length of the fixed length record
#   -stype string
#     	sort type: merge, radix, std (default "std")

vsort -reclen 32 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 |sort -c
vsort -reclen 32 -keyoff 0 -keylen 32 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 |sort -c
vsort -reclen 32 -keyoff 8 -keylen 16 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 -koff 8 -klen 16 |sort -c
vsort /tmp/rdata0 /tmp/rdata1 |sort -c

goranddatagen -n 16777216 | vsort -reclen 32 | flcat -rlen 32 | sort -c
goranddatagen -n 33554432 -rlen | vsort | sort -c

rm /tmp/brdata[01]

