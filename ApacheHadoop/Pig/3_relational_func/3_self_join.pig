divs1 = load 'NYSE_dividends' as (exchange:chararray, symbol:chararray,date:chararray, dividends);
jnd = join divs1 by symbol, divs1 by symbol;
increased = filter jnd by divs1::date < divs2::date and divs1::dividends < divs2::dividends;
dump increased;
