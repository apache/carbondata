package org.apache.spark.carbondata.streaming

import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.{CarbonSource, SparkSession}
import org.apache.spark.sql.streaming.CarbonStreamingOutputWriterFactory
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.scalatest.{BeforeAndAfterAll, FunSuite}


class CarbonSourceSchemaValidationTest extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll() {
    sql("DROP TABLE IF EXISTS _carbon_stream_table_")
  }

  test("Testing validate schema method with correct values ") {

    val spark = SparkSession.builder
      .appName("StreamIngestSchemaValidation")
      .master("local")
      .getOrCreate()

    val carbonSource = new CarbonSource
    val job = new Job()
    val storeLocation = TestQueryExecutor.storeLocation

    sql("CREATE TABLE _carbon_stream_table_(id int,name string)STORED BY 'carbondata'")
    val tablePath: String = s"$storeLocation/default/_carbon_stream_table_"
    val dataSchema = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true)))
  //  val res = carbonSource.prepareWrite(spark, job, Map("path" -> tablePath), dataSchema)
  //  assert(res.isInstanceOf[CarbonStreamingOutputWriterFactory])
  }

  override def afterAll() {
    sql("DROP TABLE IF EXISTS _carbon_stream_table_")
  }

}
