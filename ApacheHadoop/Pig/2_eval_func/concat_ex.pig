DATA = LOAD './url_data.csv' USING PigStorage(',') AS
(
 id:      int,
  countryCode:chararray,
  durationTime:int,
  url:chararray,
  keyword1:chararray,
  keyword2:chararray
);



RESULT = FOREACH DATA  {
  GENERATE
          url ,
          CONCAT(keyword1,keyword2) as combinedKeywords;
}



DUMP RESULT;
