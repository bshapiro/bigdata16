#!/bin/sh

#echo 'Running stringtie'
tmp_filename="stringtie_tmp_$$.txt"

#rm -f $tmp_filename
touch $tmp_filename

echo "@HD\tVN:1.0\tSO:coordinate" >> $tmp_filename
echo "@SQ\tSN:chrX\tLN:156040895" >> $tmp_filename

while read line
do
    echo $line >> $tmp_filename
done

#echo $tmp_filename
stringtie -p 8 $tmp_filename

#$tmp_filename
