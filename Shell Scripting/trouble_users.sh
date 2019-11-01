#!/bin/bash

# Declare a string array with type
declare -a StringArray=("20190221 20190228" "20190211 20190220" "20190201 20190210" "20190121 20190131" "20190111 20190120" "20190101 20190110" "20181221 20181231" "20181211 20181220" "20181201 20181210" "20181121 20181130" "20181111 20181120" "20181101 20181110" "20181021 20181031" "20181011 20181020" "20181001 20181010" "20180921 20180930" "20180911 20180920" "20180901 20180910" "20180816 20180831" "20180801 20180815" "20180716 20180731" "20180701 20180715" "20180616 20180630" "20180601 20180615" "20180516 20180531" "20180501 20180515" "20180416 20180430" "20180401 20180415" "20180316 20180331" "20180301 20180315" "20180216 20180228" "20180201 20180215" "20180116 20180131" "20180101 20180115")
startdate=;
enddate=;
# Read the array values with space
for val in "${StringArray[@]}";
  do
    startdate="$(cut -d' ' -f1 <<<$val)"
    enddate="$(cut -d' ' -f2 <<<$val)"
    #echo "spark-submit --master yarn --conf spark.rpc.message.maxSize=1000 --conf spark.sql.parquet.fs.optimized.committer.optimization-enabled=true --conf spark.executor.memoryOverhead=4056 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.sql.parquet.filterPushdown=true --class com.asurion.NonTroubleUsers20190618 docomo-osusume-assembly-0.1.14.jar s3a://docomo-osusume-data/Bhavani/Task17_Trouble_users_20190613/inputs/final_imei_timestamp_2017_05_2019_02.csv s3a://docomo-osusume-data/Bhavani/raw_data/appuse_literacy_log $startdate $enddate s3a://docomo-osusume-data/Bhavani/Task17_Trouble_users_20190613/outputs/non_trouble_usrs"
    echo "$startdate $enddate"
done
