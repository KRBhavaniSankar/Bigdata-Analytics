DATA = LOAD './url_data.csv' USING PigStorage(',') AS
(
  id:            int,
  countryCode:   chararray,
  durationTime:  int,
  url:           chararray,
  keyword1:      chararray,
  keyword2:      chararray
);


FILTERED_DATA = FILTER DATA BY id==8;

RESULT = FOREACH FILTERED_DATA  {
  GENERATE 
       TOKENIZE(keyword1);
}



DUMP RESULT;
