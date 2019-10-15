daily = load 'NYSE_daily' as (exchange, stock); 
grpd = group daily by stock;
cnt = foreach grpd generate group, COUNT(daily);
-- dump cnt;
thre = FILTER cnt by $1 >= 200 ;
orderby = ORDER thre by $1 desc;
top10 = LIMIT orderby 10;
store top10 into 'output_path'
