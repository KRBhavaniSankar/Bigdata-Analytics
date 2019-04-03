from pyspark.sql import SparkSession, Row
from pyspark import SparkFiles, SparkConf, SparkContext, SQLContext
from pyspark.sql.types import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    import time
    import datetime
    from datetime import datetime,date, timedelta
    import settings
    reload(settings)

    set_appname = settings.app_name
    set_master = settings.master
    docomo_accessSecretKey = settings.aws_secret_access_key
    docomo_accessKey= settings.aws_access_key_id
    log_type = settings.log_type
    start_date = settings.start_date
    end_date = settings.end_date
    output_s3_path = settings.output_s3_path

    d1 = datetime.strptime(start_date, '%Y%m%d')
    d2 = datetime.strptime(end_date, '%Y%m%d')

    docomo_s3_path =settings.docomo_s3_path

    delta = d2 - d1
    date_range = []

    for i in range(delta.days + 1):
        a = (d1 + timedelta(days=i))
        a = a.strftime('%Y%m%d').replace("-","")
        target_date = int(a)
        target_month= str(target_date/100)
        s3_a = target_month+'/'+str(target_date)+'/t_'+log_type+'_'+str(target_date)+'_*'
        date_range.append(s3_a)

    d = str(date_range).replace('[','{').replace(']','}').replace('\'',"").replace(' ','')
    s3_path = docomo_s3_path + d

    spark = SparkSession \
        .builder.master(set_master) \
        .appName(set_appname) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", docomo_accessKey)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", docomo_accessSecretKey)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    t1 = time.time()

    sample_df1 = spark.read.format("com.databricks.spark.csv")\
                            .option("header", "False")\
                            .option("inferSchema", "True")\
                            .load(s3_path)

    sample_df1.write.parquet(output_s3_path)
    t2 = time.time()

    print('Time taken to create Dataframe :: {}'.format((t2-t1)/60))

