package streaming

import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.schema.InvalidSchemaException
import org.apache.spark.sql.{CarbonSource, SparkSession}
import org.apache.spark.sql.streaming.CarbonStreamingOutputWriterFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class CarbonSourceTest extends FunSuite with MockitoSugar {

  val spark = SparkSession.builder
    .appName("StructuredNetworkWordCount")
    .master("local")
    .getOrCreate()
  test("Testing validate schema method with correct values ") {

    val carbonSource = new CarbonSource
    val job = new Job()
    val dataSchema = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true)))
    val res = carbonSource.prepareWrite(spark, job, Map("checkpointlocation" -> "/home/hduser/ckpt", "path" -> "/home/hduser/tables/default/streaming"), dataSchema)
    Console.println("Asserted value : " + res)
    assert(res.isInstanceOf[CarbonStreamingOutputWriterFactory])
  }


  test("Testing validate schema method with incorrect values : Should throw an exception ") {
    val carbonSource = new CarbonSource
    val job = new Job()
    val dataSchema = StructType(Array(StructField("id", IntegerType, true)))
    intercept[InvalidSchemaException] {
      carbonSource.prepareWrite(spark, job, Map("checkpointlocation" -> "/home/hduser/ckpt", "path" -> "/home/hduser/tables/default/streaming"), dataSchema)
    }
  }
}