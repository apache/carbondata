package org.apache.carbondata.spark.testsuite.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

class ComplexTypeUdfTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table_udf")
    sql(
      s"""
         | CREATE TABLE carbon_table_udf(
         | intField INT,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | arrayData ARRAY<STRING>,
         | structData STRUCT<imei:STRING,
         |                   imsi:STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val path = s"$resourcesPath/udf.csv"

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table_udf
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on
  }

  test("test builtin function") {
    checkAnswer(sql("select explode(arrayData) from carbon_table_udf"), Seq(Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'"),
      Row("'foo'"), Row("'bar'"), Row("'world'")))
  }

  test("primitive data type with udf") {
    sqlContext.udf.register("appendUDF", (value: String) => value.concat("-UDF"))
    checkAnswer(sql("select appendUDF(stringField) from carbon_table_udf"), Seq(Row("spark-UDF"),
      Row("spark-UDF"),
      Row("flink-UDF"),
      Row("spark-UDF"),
      Row("spark-UDF"),
      Row("hive-UDF"),
      Row("impala-UDF"),
      Row("spark-UDF"),
      Row("impala-UDF"),
      Row("spark-UDF")))
  }

  test("array data with udf") {
    sql("DROP temporary function if exists UDFArrayFunc")
    // appends some text to array first values
    sql(s"create temporary function UDFArrayFunc AS 'org.apache.carbondata.integration.spark.testsuite.UDFArray'")
    checkAnswer(sql(s"""SELECT UDFArrayFunc(arrayData) from carbon_table_udf""".stripMargin), Seq(Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF"),
      Row("'foo'-UDF")))
  }

  import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

  test("struct data with udf") {
    sql("DROP temporary function if exists UDFStructFunc")
    // appends some text to struct values
    sql(s"create temporary function UDFStructFunc AS 'org.apache.carbondata.integration.spark.testsuite.UDFStruct'")
    checkAnswer(sql(s"""SELECT UDFStructFunc(structData) from carbon_table_udf""".stripMargin), Seq(
      Row(new GenericRowWithSchema(Array("1AA100-UDF", "2BB100-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1100-UDF", "2BB1100-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1200-UDF", "2BB1200-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1300-UDF", "2BB1300-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1400-UDF", "2BB1400-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1500-UDF", "2BB1500-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1600-UDF", "2BB1600-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1700-UDF", "2BB1700-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1800-UDF", "2BB1800-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType))))),
      Row(new GenericRowWithSchema(Array("1AA1900-UDF", "2BB1900-UDF"), StructType(Array(StructField("imei", StringType), StructField("imsi", StringType)))))))
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table_udf")
    sql("DROP temporary function if exists UDFArrayFunc")
    sql("DROP temporary function if exists UDFStructFunc")
  }
}
