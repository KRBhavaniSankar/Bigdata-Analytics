-- For each stock, find all dividends that increased between two dates
divs1 = load 'NYSE_dividends' as (exchange:chararray, symbol:chararray, date:chararray, dividends);
divs2 = load 'NYSE_dividends' as (exchange:chararray, symbol:chararray, date:chararray, dividends);
jnd = join divs1 by symbol , divs2 by symbol;
increased = filter jnd by divs1::date < divs2::date and divs1::dividends < divs2::dividends;
dump increased;
