hadoop fs -rm /user/cloudera/input/*

#!/usr/bin/bash
filename="$1"
counter=0
while read -r line
do
    OUT=$counter
    name="$line"
    echo "$name" > $OUT
    hadoop fs -copyFromLocal $OUT /user/cloudera/input/
    rm $OUT
    sleep 8
    counter=$((counter+1))
done < "$filename"
