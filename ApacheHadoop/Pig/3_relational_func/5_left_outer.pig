daily = load 'NYSE_daily' as (exchange, symbol, date, open, high, low, close, volume, adj_close);
divs = load 'NYSE_dividends' as (exchange, symbol, date, dividends); 
jnd = join daily by (symbol, date) left outer, divs by (symbol, date);
dump jnd;

