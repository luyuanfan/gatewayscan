#!/bin/bash
set -e 
source_dir="/mnt/usb"
tmp_dir="/dbdata"

for filename in `ls $source_dir`; do
    suffix="${filename#*.}"
    if [ $suffix != "csv.bz2" ]; then
        continue
    fi
    decompressed="${filename%.*}"
    echo "Decompressing $source_dir/$filename to $tmp_dir/$decompressed"
    pbzip2 -dckf -p4 "$source_dir/$filename" > "$tmp_dir/$decompressed"
    echo "Done decompressing $source_dir/$filename"
done

echo "Done decompressing everything"