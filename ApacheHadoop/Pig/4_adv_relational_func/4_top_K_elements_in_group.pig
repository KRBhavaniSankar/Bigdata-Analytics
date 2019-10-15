divs = load '/Users/bhavani.sankar/Desktop/Bhavani/ApachePig/ProgrammingPig/programmingpig-master/data/NYSE_dividends' as (exchange:chararray, symbol:chararray, date:chararray, dividends:float);
grpd = group divs by symbol; 
top3 = foreach grpd {
sorted = order divs by dividends desc; 
top = limit sorted 3;
generate group, flatten(top);
};
dump top3;

