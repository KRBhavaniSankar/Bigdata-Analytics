
players= load 'baseball' as (name:chararray, team:chararray, position:bag{t:(p:chararray)}, bat:map[]);
noempty= foreach players generate name,
((position is null or IsEmpty(position)) ? {('unknown')} : position) as position;
pos= foreach noempty generate name, flatten(position) as position; 
bypos= group pos by position;
