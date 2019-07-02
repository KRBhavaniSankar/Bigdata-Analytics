startdate=$1
enddate=$2

curr_date=;
curr_month=;
n=0
until [ "$curr_date" = "$enddate" ]
do
    curr_date=$(date -d "$startdate + $n months" +%Y%m%d)
    curr_month=$(date -d "$startdate + $n months" +%Y%m)
    echo "$curr_month
    ((n--))
done
