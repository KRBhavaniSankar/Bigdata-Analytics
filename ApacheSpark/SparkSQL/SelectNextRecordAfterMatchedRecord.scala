import org.apache.spark.sql.SparkSession

object ConsecutiveRecordSample extends App{


  val sampleData1 = args(0)
  val sampleData2 = args(1)

  val spark = SparkSession.builder()
    .appName(s"ConsecutiveRecords : ${args.mkString(", ")}")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  val imeiDF = spark.read.option("header","true").csv(sampleData1)
  val appUseDF = spark.read.option("header","true").csv(sampleData2)
  imeiDF.createOrReplaceTempView("imei_data")
  appUseDF.createOrReplaceTempView("app_use_data")

  val query ="""
            SELECT A.*,B.*
            FROM imei_data A INNER JOIN app_use_data B
            ON A.ID = B.ID
            WHERE B.N1 = A.N1
            """
  //val resultDF = spark.sql(query)

  //val query2 = "SELECT * FROM numbers WHERE id IN (SELECT ID+1 FROM numbers WHERE N1 = 9)"

  val query2="""
            SELECT * FROM app_use_data
            WHERE ID IN
            (SELECT B.ID+1
            FROM imei_data A INNER JOIN app_use_data B
            ON A.ID = B.ID
            WHERE B.N1 = A.N1)
            """
  val res2 = spark.sql(query2)

  val query3 = """
            SELECT child.* FROM app_use_data as child,
            (SELECT B.* FROM imei_data A INNER JOIN app_use_data B
            ON A.ID = B.ID
            WHERE A.N1 = B.N1) as parent
            WHERE child.ID <= parent.ID+5
            """
  res2.show(10,false)
  res2.printSchema()

  val res3 = spark.sql(query3)
  res3.show(10,false)
  res3.printSchema()
}
