#!/bin/bash -e                                                                                                                                                                                               

IN=$1
OUT=$2

STRING="\{\"index\":\{\"\_index\":\"game\", \"_type\": \"eachGame\", \"_id\":\"1\"\}\}\n"
sed "s/^/${STRING}/g" $IN | awk '{gsub("\"1\"",'NR',$0);print}' > $OUT
