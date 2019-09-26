/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.testsuite.dataload

import java.io.File
import java.math.BigDecimal

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.scalatest.BeforeAndAfterAll

class TestLoadDataFrame extends QueryTest with BeforeAndAfterAll {
  var df: DataFrame = _
  var dataFrame: DataFrame = _
  var df2: DataFrame = _
  var df3: DataFrame = _
  var booldf:DataFrame = _


  def buildTestData() = {
    import sqlContext.implicits._
    df = sqlContext.sparkContext.parallelize(1 to 32000)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "c3")

    val rdd = sqlContext.sparkContext.parallelize(
      Row(52.23, BigDecimal.valueOf(1234.4440), "Warsaw") ::
      Row(42.30, BigDecimal.valueOf(9999.9990), "Corte") :: Nil)

    val schema = StructType(
      StructField("double", DoubleType, nullable = false) ::
      StructField("decimal", DecimalType(9, 2), nullable = false) ::
      StructField("string", StringType, nullable = false) :: Nil)

    dataFrame = sqlContext.createDataFrame(rdd, schema)
    df2 = sqlContext.sparkContext.parallelize(1 to 1000)
      .map(x => ("key_" + x, "str_" + x, x, x * 2, x * 3))
      .toDF("c1", "c2", "c3", "c4", "c5")

    df3 = sqlContext.sparkContext.parallelize(1 to 3)
      .map(x => (x.toString + "te,s\nt", x))
      .toDF("c1", "c2")

    val boolrdd = sqlContext.sparkContext.parallelize(
      Row("anubhav",true) ::
        Row("prince",false) :: Nil)

    val boolSchema = StructType(
      StructField("name", StringType, nullable = false) ::
        StructField("isCarbonEmployee",BooleanType,nullable = false)::Nil)
    booldf = sqlContext.createDataFrame(boolrdd,boolSchema)
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
    sql("DROP TABLE IF EXISTS carbon3")
    sql("DROP TABLE IF EXISTS carbon4")
    sql("DROP TABLE IF EXISTS carbon5")
    sql("DROP TABLE IF EXISTS carbon6")
    sql("DROP TABLE IF EXISTS carbon7")
    sql("DROP TABLE IF EXISTS carbon8")
    sql("DROP TABLE IF EXISTS carbon9")
    sql("DROP TABLE IF EXISTS carbon10")
    sql("DROP TABLE IF EXISTS carbon11")
    sql("DROP TABLE IF EXISTS carbon12")
    sql("DROP TABLE IF EXISTS df_write_sort_column_not_specified")
    sql("DROP TABLE IF EXISTS df_write_specify_sort_column")
    sql("DROP TABLE IF EXISTS df_write_empty_sort_column")
    sql("DROP TABLE IF EXISTS carbon_table_df")
    sql("DROP TABLE IF EXISTS carbon_table_df1")
  }



  override def beforeAll {
    dropTable
    buildTestData
  }

  test("test the boolean data type"){
    booldf.write
      .format("carbondata")
      .option("tableName", "carbon0")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("SELECT * FROM CARBON0"),
      Seq(Row("anubhav", true), Row("prince", false)))
  }

  test("test load dataframe with saving compressed csv files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon1 where c3 > 500"), Row(31500)
    )
    sql(s"describe formatted carbon1").show(true)
  }

  test("test load dataframe with saving csv uncompressed files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .option("tempCSV", "true")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon2 where c3 > 500"), Row(31500)
    )
  }

  test("test load dataframe without saving csv files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon3")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon3 where c3 > 500"), Row(31500)
    )
  }

  test("test decimal values for dataframe load"){
    dataFrame.write
      .format("carbondata")
      .option("tableName", "carbon4")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("SELECT decimal FROM carbon4"),Seq(Row(BigDecimal.valueOf(10000.00)),Row(BigDecimal.valueOf(1234.44))))
  }

  test("test loading data if the data count is multiple of page size"){
    checkAnswer(
      sql("SELECT count(*) FROM carbon2"),Seq(Row(32000)))
  }

  test("test load dataframe with integer columns included in the dictionary"){
    df2.write
      .format("carbondata")
      .option("tableName", "carbon5")
      .option("compress", "true")
      .option("dictionary_include","c3,c4")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon5 where c3 > 300"), Row(700)
    )
  }

  test("test load dataframe with string column excluded from the dictionary"){
    df2.write
      .format("carbondata")
      .option("tableName", "carbon6")
      .option("compress", "true")
      .option("dictionary_exclude","c2")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon6 where c3 > 300"), Row(700)
    )
  }

  test("test load dataframe with both dictionary include and exclude specified"){
    df2.write
      .format("carbondata")
      .option("tableName", "carbon7")
      .option("compress", "true")
      .option("dictionary_include","c3,c4")
      .option("dictionary_exclude","c2")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon7 where c3 > 300"), Row(700)
    )
  }

  test("test load dataframe with single pass enabled") {
    // save dataframe to carbon file
    df2.write
      .format("carbondata")
      .option("tableName", "carbon8")
      .option("tempCSV", "false")
      .option("single_pass", "true")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon8 where c3 > 500"), Row(500)
    )
  }

  test("test load dataframe with single pass disabled") {
    // save dataframe to carbon file
    df2.write
      .format("carbondata")
      .option("tableName", "carbon9")
      .option("tempCSV", "true")
      .option("single_pass", "false")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon9 where c3 > 500"), Row(500)
    )
  }

  test("test datasource table with specified table path") {
    val path = "./source"
    df2.write
      .format("carbondata")
      .option("tableName", "carbon10")
      .option("tablePath", path)
      .mode(SaveMode.Overwrite)
      .save()
    assert(new File(path).exists())
    checkAnswer(
      sql("select count(*) from carbon10 where c3 > 500"), Row(500)
    )
    sql("drop table carbon10")
    assert(!new File(path).exists())
    assert(intercept[AnalysisException](
      sql("select count(*) from carbon10 where c3 > 500"))
      .message
      .contains("not found"))
  }
  test("test streaming Table") {
    dataFrame.write
      .format("carbondata")
      .option("tableName", "carbon11")
      .option("tempCSV", "true")
      .option("single_pass", "false")
      .option("compress", "false")
      .option("streaming", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("SELECT decimal FROM carbon11"),Seq(Row(BigDecimal.valueOf(10000.00)),Row(BigDecimal.valueOf(1234.44))))
    val descResult =sql("desc formatted carbon11")
    val isStreaming: String = descResult.collect().find(row=>row(0).asInstanceOf[String].trim.equalsIgnoreCase("streaming")).get.get(1).asInstanceOf[String]
    assert(isStreaming.contains("sink"))
  }

  test("test datasource table with specified char") {

    df3.write
      .format("carbondata")
      .option("tableName", "carbon12")
      .option("tempCSV", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon12"), Row(3)
    )
  }
  private def getSortColumnValue(tableName: String): Array[String] = {
    val desc = sql(s"desc formatted $tableName")
    val sortColumnRow = desc.collect.find(r =>
      r(0).asInstanceOf[String].trim.equalsIgnoreCase("Sort Columns")
    )
    assert(sortColumnRow.isDefined)
    sortColumnRow.get.get(1).asInstanceOf[String].split(",")
      .map(_.trim.toLowerCase).filter(_.length > 0)
  }

  private def getDefaultWriter(tableName: String): DataFrameWriter[Row] = {
    df2.write
      .format("carbondata")
      .option("tableName", tableName)
      .option("tempCSV", "false")
      .option("single_pass", "false")
      .option("table_blocksize", "256")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
  }

  // now by default all the dimensions are not selected for sorting in no_sort
  ignore("test load dataframe with sort_columns not specified," +
       " by default all string columns will be sort_columns") {
    // all string column will be sort_columns by default
    getDefaultWriter("df_write_sort_column_not_specified").save()
    checkAnswer(
      sql("select count(*) from df_write_sort_column_not_specified where c3 > 500"), Row(500)
    )

    val sortColumnValue = getSortColumnValue("df_write_sort_column_not_specified")
    assert(sortColumnValue.sameElements(Array("c1", "c2")))
  }

  test("test load dataframe with sort_columns specified") {
    // only specify c1 as sort_columns
    getDefaultWriter("df_write_specify_sort_column").option("sort_columns", "c1").save()
    checkAnswer(
      sql("select count(*) from df_write_specify_sort_column where c3 > 500"), Row(500)
    )

    val sortColumnValue = getSortColumnValue("df_write_specify_sort_column")
    assert(sortColumnValue.sameElements(Array("c1")))
  }

  test("test load dataframe with sort_columns specified empty") {
    // specify empty sort_column
    getDefaultWriter("df_write_empty_sort_column").option("sort_columns", "").save()
    checkAnswer(
      sql("select count(*) from df_write_empty_sort_column where c3 > 500"), Row(500)
    )

    val sortColumnValue = getSortColumnValue("df_write_empty_sort_column")
    assert(sortColumnValue.isEmpty)
  }

  test("test load dataframe while giving already created table") {

    sql(s"create table carbon_table_df(c1 string, c2 string, c3 int) stored by 'carbondata'")
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon_table_df")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()

    df.write
      .format("carbondata")
      .option("tableName", "carbon_table_df")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon_table_df where c3 > 500"), Row(31500)
    )
  }

  test("test load dataframe while giving already created table with delete segment") {

    sql(s"create table carbon_table_df1(c1 string, c2 string, c3 int) stored by 'carbondata'")
    val table = CarbonEnv.getCarbonTable(TableIdentifier("carbon_table_df1"))(sqlContext.sparkSession)
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon_table_df1")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()

    assert(CarbonEnv.getCarbonTable(TableIdentifier("carbon_table_df1"))(sqlContext.sparkSession)
      .getTableInfo.getFactTable.equals(table.getTableInfo.getFactTable))

    sql("delete from table carbon_table_df1 where segment.id in (0)")
    df.write
      .format("carbondata")
      .option("tableName", "carbon_table_df1")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
    assert(CarbonEnv.getCarbonTable(TableIdentifier("carbon_table_df1"))(sqlContext.sparkSession)
      .getTableInfo.getFactTable.equals(table.getTableInfo.getFactTable))
    checkAnswer(
      sql("select count(*) from carbon_table_df1 where c3 > 500"), Row(31500)
    )

  }

  test("test load data with binary_decoder in df") {
    val spark = sqlContext.sparkSession
    try {
      sql("DROP TABLE IF EXISTS carbon_table")
      val rdd = spark.sparkContext.parallelize(1 to 3)
              .map(x => Row("a" + x % 10, "b", x, "YWJj".getBytes()))
      val customSchema = StructType(Array(
        StructField("c1", StringType),
        StructField("c2", StringType),
        StructField("number", IntegerType),
        StructField("c4", BinaryType)))

      val df = spark.createDataFrame(rdd, customSchema);
      // Saves dataFrame to carbondata file
      df.write.format("carbondata")
              .option("binary_decoder", "base64")
              .option("tableName", "carbon_table")
              .save()

      val carbonDF = spark.read
              .format("carbondata")
              .option("tableName", "carbon_table")
              .schema(customSchema)
              .load()

      assert(carbonDF.schema.map(_.name) === Seq("c1", "c2", "number", "c4"))
      // "YWJj" is base64 decode data of "abc"
      checkAnswer(carbonDF, Seq(Row("a1", "b", 1, "abc".getBytes()),
        Row("a2", "b", 2, "abc".getBytes()),
        Row("a3", "b", 3, "abc".getBytes())))

      val carbonDF2 = carbonDF.drop("c1")
      assert(carbonDF2.schema.map(_.name) === Seq("c2", "number", "c4"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false)
    } finally {
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  override def afterAll {
    dropTable
  }
}

