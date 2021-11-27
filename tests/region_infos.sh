#!/usr/bin/env bash

i=1
grep "region_infos" recover.log | awk '{ $1=$2=""; print $0 }' | sed 's/  msg=\"//g' | sed 's/\\n\"//g' |
while IFS= read -r line; do
  echo "$line" | sed 's/\\n/\n/g' | sed 's/\\"/\"/g' > $i.json
  ((i=i+1))
done
