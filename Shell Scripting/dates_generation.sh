startdate=$1
enddate=$2
curr_date=;
n=0
until [ "$curr_date" = "$enddate" ]
do
    curr_date=$(date -d "$startdate + $n days" +%Y%m%d)
    echo $curr_date
    ((n++))
done
