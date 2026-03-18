#!/bin/bash

source_dir='/mnt/usb'
tmp_dir='/dbdata'

mkdir -p "$tmp_dir"c

for f in "$source_dir"/*.csv.bz2; do
    filename=$(basename "$f" .bz2)
    echo "Decompressing $f to $tmp_dir/$filename"
    pbzip2 -dckf -p30 "$f" > "$tmp_dir/$filename"
done

echo "Done decompressing"