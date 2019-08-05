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
       MAX(DATA.durationTime) as maxDurationTime,
       MIN(DATA.durationTime) as minDurationTime;
}

RES_SIZE = FOREACH DATA  {
  GENERATE
      keyword1,
       SIZE(keyword1) as numberOfCharacher;
}

DUMP RESULT;
DUMP RES_SIZE ;
