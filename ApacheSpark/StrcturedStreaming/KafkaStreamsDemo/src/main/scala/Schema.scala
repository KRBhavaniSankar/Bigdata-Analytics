package com.bhavani.spark.streaming.kafka
import org.apache.spark.sql.types._
object Schema {

  val schema = StructType(
    List(
    StructField("InvoiceNumber", StringType),
    StructField("CreatedTime", LongType),
    StructField("StoreID", StringType),
    StructField("PosID", StringType),
    StructField("CashierID", StringType),
    StructField("CustomerType", StringType),
    StructField("CustomerCardNo", StringType),
    StructField("TotalAmount", DoubleType),
    StructField("NumberOfItems", IntegerType),
    StructField("PaymentMethod", StringType),
    StructField("CGST", DoubleType),
    StructField("SGST", DoubleType),
    StructField("CESS", DoubleType),
    StructField("DeliveryType", StringType),
    StructField("DeliveryAddress", StructType(List(
      StructField("AddressLine", StringType),
      StructField("City", StringType),
      StructField("State", StringType),
      StructField("PinCode", StringType),
      StructField("ContactNumber", StringType)
    ))),
    StructField("InvoiceLineItems", ArrayType(StructType(List(
      StructField("ItemCode", StringType),
      StructField("ItemDescription", StringType),
      StructField("ItemPrice", DoubleType),
      StructField("ItemQty", IntegerType),
      StructField("TotalValue", DoubleType)
    )))),
  ))


}
