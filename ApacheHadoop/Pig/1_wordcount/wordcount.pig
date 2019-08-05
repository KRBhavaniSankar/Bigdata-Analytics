/*
-start pig shell using ' pig -x LOCAL' command
This is script to count no.of word occurance in an file
Developed by : Bhavani , Data science engineer
*/
-- LOAD the input file from HDFS location
Lines = LOAD 'hdfs://localhost:9000/ApachePig/1_wordcount/sample_input.txt' AS (line: chararray);

-- GENERATE each line into a word using FLATTEN
Words = FOREACH Lines GENERATE FLATTEN (TOKENIZE (line)) AS word;

-- Group each word
Groups = GROUP Words BY word;

-- Count each word and generate word, count values
Counts = FOREACH Groups GENERATE group, COUNT (Words);

-- Save the results into HDFS.
STORE Counts INTO 'hdfs://localhost:9000/ApachePig/1_wordcount/output/' USING PigStorage(',');
