DATA = LOAD './url_data.csv' USING PigStorage(',') AS
(
  id:            int,
  countryCode:   chararray,
  durationTime:  int,
  url:           chararray,
  keyword1:      chararray,
  keyword2:      chararray
);


GROUPED_DATA = GROUP DATA BY countryCode;


RESULT = FOREACH GROUPED_DATA  {
  GENERATE 
       group ,
       SUM(DATA.durationTime) as totalDurationTime;
}



DUMP RESULT;
