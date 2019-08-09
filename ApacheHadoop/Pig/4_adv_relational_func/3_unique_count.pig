daily = load 'NYSE_daily' as (exchange, symbol); -- not interested in other fields 
grpd = group daily by exchange;
uniqcnt = foreach grpd {

sym = daily.symbol; 
uniq_sym = distinct sym; 
generate group, COUNT(uniq_sym);
};

dump uniq_sym;
