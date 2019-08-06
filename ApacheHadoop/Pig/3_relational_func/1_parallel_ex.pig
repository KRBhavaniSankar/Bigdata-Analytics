daily = load '/Users/bhavani.sankar/Desktop/Bhavani/ApachePig/ProgrammingPig/programmingpig-master/data/NYSE_daily' as (exchange, symbol, date, open, high, low, close, volume, adj_close);
bysymbl = group daily by symbol parallel 10;
average = foreach bysymbl generate group, AVG(daily.close) as avg;
sorted = order average by avg desc parallel 2;
-- store sorted into './parallel_res';
store bysymbl into './parallel_res_10';
