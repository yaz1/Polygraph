#!/bin/bash
#{"topic":"foo1","partition":2,"replicas":[1]},
file="/home/mr1/partition.txt"
printf "{\"version\":1,\"partitions\":[\n" >$file

topic=$1
counter=0
numV=$2
numS=$3


while [ $counter -lt $numV ]
do
partition=$((counter%numS))
printf "{\"topic\":\"$topic\",\"partition\":$counter,\"replicas\":[$partition]},\n" >>$file
for i in 1 2 3
do
counter2=$((numV*i+counter))
if [ $counter2 -lt $(($numV*4-1)) ]
then
printf "{\"topic\":\"$topic\",\"partition\":$counter2,\"replicas\":[$partition]},\n" >>$file
else
printf "{\"topic\":\"$topic\",\"partition\":$counter2,\"replicas\":[$partition]}\n" >>$file
fi
done

((counter++))
done
printf "]}" >>$file




