define hex InvokeForString('java.lang.Integer.toHexString', 'int'); 
divs = load '/Users/bhavani.sankar/Desktop/Bhavani/ApachePig/ProgrammingPig/programmingpig-master/data/NYSE_daily' as (exchange, symbol, date, open, high, low,close, volume, adj_close); 
nonnull = filter divs by volume is not null;
inhex = foreach nonnull generate symbol, hex((int)volume);

dump inhex;
