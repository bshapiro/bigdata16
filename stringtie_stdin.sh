#!/bin/sh

tmp_filename="stringtie_tmp.txt"

rm -f $tmp_filename
touch $tmp_filename

while read line
do
    echo $line >> $tmp_filename
done

stringtie -p 8 $tmp_filename

rm $tmp_filename
