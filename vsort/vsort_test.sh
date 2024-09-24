#! /bin/sh
set -ex

############ to generate random data for testing:
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
########### to format fixed length data w/o delimiters for sort -c:
# https://github.com/dfwcnj/flcat
# Usage of flcat:
# #   -fn string
# #     	name of fl file to emit
# #   -klen int
# #     	record key length
# #   -koff int
# #     	offset of key in record
# #   -rlen int
# #     	record length
# ##############
# # https://github.com/dfwcnj/govbinsort
# Usage of ./vsort:
#   -form string
#     	data form bytes or string (default "string")
#   -iomem string
#     	max read memory size in kb, mb or gb (default "500mb")
#   -keylen int
#     	length of the key if not whole line
#   -keyoff int
#     	offset of the key
#   -md string
#     	merge sirectory defaults to a directory under /tmp
#   -ofn string
#     	output file name otherwise stdout
#   -reclen int
#     	length of the fixed length record
#   -stype string
#     	sort type: merge, radix, std (default "std")

if ! test -f vsort; then
    go mod tidy
    go build
fi

# remove any previous intermediate data
# rm -r /tmp/[fmSs]*
# rm /tmp/[br]data[01]

# generate file based data
goranddatagen -n 16777216  >/tmp/bdata0
goranddatagen -n 16777216  >/tmp/bdata1
goranddatagen -n 33554432 -rlen >/tmp/rdata0
goranddatagen -n 33554432 -rlen >/tmp/rdata1

# fixed length bytes sort
./vsort -reclen 32 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 |sort -c
./vsort -form bytes -reclen 32 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 |sort -c

# fixed length bytes sort easiest key
./vsort -reclen 32 -keyoff 0 -keylen 32 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 |sort -c
./vsort -form bytes -reclen 32 -keyoff 0 -keylen 32 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 |sort -c

# fixed length bytes sort subrecord key
./vsort -reclen 32 -keyoff 8 -keylen 16 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 -koff 8 -klen 16 |sort -c
./vsort -form bytes -reclen 32 -keyoff 8 -keylen 16 /tmp/bdata0 /tmp/bdata1 | flcat -rlen 32 -koff 8 -klen 16 |sort -c

# random length data sort
./vsort /tmp/rdata0 /tmp/rdata1 |sort -c

rm /tmp/[br]data[01]

# fixed length standard input
goranddatagen -n 16777216 | ./vsort -reclen 32 | flcat -rlen 32 | sort -c
goranddatagen -n 16777216 | ./vsort -reclen 32 -form bytes | flcat -rlen 32 | sort -c

goranddatagen -n 16777216 | ./vsort -reclen 32 -stype heap | flcat -rlen 32 | sort -c
goranddatagen -n 16777216 | ./vsort -reclen 32 -stype heap -form bytes | flcat -rlen 32 | sort -c

goranddatagen -n 65536 | ./vsort -reclen 32 -stype insertion | flcat -rlen 32 | sort -c
goranddatagen -n 65536 | ./vsort -reclen 32 -stype insertion -form bytes | flcat -rlen 32 | sort -c

# goranddatagen -n 16777216 | ./vsort -reclen 32  -stype merge | flcat -rlen 32 | sort -c
# goranddatagen -n 16777216 | ./vsort -reclen 32  -stype merge -form bytes | flcat -rlen 32 | sort -c

goranddatagen -n 16777216 | ./vsort -reclen 32  -stype radix | flcat -rlen 32 | sort -c
goranddatagen -n 16777216 | ./vsort -reclen 32  -stype radix -form bytes | flcat -rlen 32 | sort -c

# random length standard input
goranddatagen -n 33554432 -rlen | ./vsort | sort -c
goranddatagen -n 33554432 -rlen | ./vsort -stype heap | sort -c
goranddatagen -n 65536 -rlen | ./vsort -stype insertion | sort -c
goranddatagen -n 33554432 -rlen | ./vsort -stype merge | sort -c
goranddatagen -n 33554432 -rlen | ./vsort -stype radix | sort -c

# failing
goranddatagen -n 16777216 | ./vsort -reclen 32  -stype merge -form bytes | flcat -rlen 32 | sort -c
goranddatagen -n 16777216 | ./vsort -reclen 32  -stype merge | flcat -rlen 32 | sort -c


rm -r /tmp/[Ss]*

