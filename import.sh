#!/bin/bash
while read code time
do
echo nohup mongoexport -d stock -c mins -q "{code:'$code', time:{\$gte:ISODate('$time')}}" --fieldFile mins_fields.txt --type=csv --out "exportdir/mins_"$code"_"$time".csv" &
nohup mongoexport -d stock -c mins -q "{code:'$code', time:{\$gte:ISODate('$time')}}" --fieldFile mins_fields.txt --type=csv --out "exportdir/mins_"$code"_"$time".csv" &
done<input.txt