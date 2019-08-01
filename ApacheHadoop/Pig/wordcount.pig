/*
-start pig shell using ' pig -x LOCAL' command
This is script to count no.of word occurance in an file
Developed by : Bhavani , Data science engineer
*/
Lines = LOAD 'hdfs://localhost:9000/ApachePig/1_wordcount/sample_input.txt' AS (line: chararray);
Words = FOREACH Lines GENERATE FLATTEN (TOKENIZE (line)) AS word;
Groups = GROUP Words BY word;
Counts = FOREACH Groups GENERATE group, COUNT (Words);
STORE Counts INTO 'hdfs://localhost:9000/ApachePig/1_wordcount/output/' USING PigStorage(',');
