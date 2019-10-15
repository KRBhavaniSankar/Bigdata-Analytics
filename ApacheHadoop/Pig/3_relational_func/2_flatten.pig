crawl = load '/Users/bhavani.sankar/Desktop/Bhavani/ApachePig/ProgrammingPig/programmingpig-master/data/webcrawl' as (url, pageid);
extracted = foreach crawl generate flatten(REGEX_EXTRACT_ALL(url,'(http|https)://(.*?)/(.*)')) as (protocol, host, path);
describe extracted;
dump extracted;
