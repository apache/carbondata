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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.{File, PrintWriter}
import java.util.{TimeZone, UUID}

import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.{CarbonSchemaReader, CarbonWriterBuilder}

@Ignore
class SparkStoreCreatorForPresto extends QueryTest with BeforeAndAfterAll{

  private val timestampFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
  private val dateFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT)
  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val sparkStorePath = s"$rootPath/integration/spark/target/spark_store"

  val storePath = storeLocation

  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop database if exists presto_spark_db cascade")
    sql("create database presto_spark_db")
    sql("use presto_spark_db")
     CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile
        (s"$sparkStorePath"))
  }

  override def afterAll: Unit = {
    if (null != dateFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, dateFormat)
    }
    if(null != timestampFormat) {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
    }
    val source = s"$rootPath/integration/spark/target/warehouse/presto_spark_db.db/"
    val srcDir = new File(source)

    // Presto will later use this store path to query
    val destination = s"$rootPath/integration/spark/target/spark_store/"
    val destDir = new File(destination)
    FileUtils.copyDirectory(srcDir, destDir)
    FileUtils.deleteDirectory(srcDir)
    sql("drop table if exists update_table")
    sql("drop table if exists actual_update_table")
    sql("drop table if exists iud_table")
    sql("drop table if exists testmajor")
    sql("drop table if exists minor_compaction")
    sql("drop table if exists custom_compaction_table")
    sql("drop table if exists segment_table")
    sql("drop table if exists delete_segment_table")
    sql("drop table if exists inv_table")
    sql("drop table if exists partition_table")
    sql("drop table if exists carbon_normal")
    sql("drop table if exists carbon_bloom")
    sql("drop table if exists range_table")
    sql("drop table if exists streaming_table")
    sql("drop table if exists array_decimal")
    sql("drop table if exists struct_decimal")
    sql("drop table if exists array_short")
    sql("drop table if exists array_int")
    sql("drop table if exists array_long")
    sql("drop table if exists array_double")
    sql("drop table if exists array_timestamp")
    sql("use default ")
  }

  test("Test update operations without local dictionary") {
    sql("drop table if exists update_table")
    sql("drop table if exists actual_update_table")
    sql(
      "CREATE TABLE IF NOT EXISTS update_table (smallintColumn short, intColumn " +
      "int, bigintColumn bigint, doubleColumn " +
      "double, decimalColumn decimal(10,3)," +
      "timestampColumn timestamp, dateColumn date, " +
      "stringColumn string, booleanColumn boolean) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='false')"
    )
    sql(
      "insert into update_table values(1, 2, 3333333, 4.1,5.1,'2017-01-01 12:00:00.0', " +
      "'2017-09-08','abc',true)")
    sql(
      "CREATE TABLE IF NOT EXISTS actual_update_table (smallintColumn short, intColumn " +
      "int, bigintColumn bigint, doubleColumn " +
      "double, decimalColumn decimal(10,3)," +
      "timestampColumn timestamp, dateColumn date, " +
      "stringColumn string, booleanColumn boolean) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='false')"
    )
    sql(
      "insert into actual_update_table values(11, 22, 39999, 4.4,5.5,'2020-01-11 12:00:45.0', " +
      "'2020-01-11','defgh',false)")

    sql("update update_table set (smallintColumn) = (11)")
    sql("update update_table set (intColumn) = (22)")
    sql("update update_table set (bigintColumn) = (39999)")
    sql("update update_table set (doubleColumn) = (4.4)")
    sql("update update_table set (decimalColumn) = (5.5)")
    sql("update update_table set (timestampColumn) = ('2020-01-11 12:00:45.0')")
    sql("update update_table set (dateColumn) = ('2020-01-11')")
    sql("update update_table set (stringColumn) = ('defgh')")
    sql("update update_table set (booleanColumn) = (false)")
  }

  test("Test delete operations") {
    sql("drop table if exists iud_table")
    sql(
      "CREATE TABLE IF NOT EXISTS iud_table (smallintColumn short, intColumn " +
      "int, bigintColumn bigint, doubleColumn " +
      "double, decimalColumn decimal(10,3), " +
      "timestampColumn timestamp, dateColumn date, " +
      "stringColumn string, booleanColumn boolean) STORED AS carbondata"
    )
    sql(
      "insert into iud_table values(1, 2, 3333333, 4.1,5.1,'2017-01-01 12:00:00.0', '2017-09-08'," +
      "'row1',true)")
    sql(
      "insert into iud_table values(32, 33, 3555555, 4.1,5.1,'2017-01-01 12:00:00.0', " +
      "'2017-05-05','row2',false)")
    sql(
      "insert into iud_table values(42, 43, 4555555, 4.15,5.15,'2017-01-01 12:00:00.0', " +
      "'2017-05-05','row3',true)")
    sql("DELETE FROM iud_table WHERE smallintColumn = 32").show()
  }

  test("Test major compaction") {
    sql("drop table if exists testmajor")
    sql(
      "CREATE TABLE IF NOT EXISTS testmajor (country String, arrayInt array<int>) STORED AS " +
      "carbondata"
    )
    sql("insert into testmajor select 'India', array(1,2,3) ")
    sql("insert into testmajor select 'China', array(1,2) ")
    // compaction will happen here.
    sql("alter table testmajor compact 'major'")
    sql("insert into testmajor select 'Iceland', array(4,5,6) ")
    sql("insert into testmajor select 'Egypt', array(4,5) ")

    sql("alter table testmajor compact 'major'")
  }

  test("Test minor compaction") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2")
      sql("DROP TABLE IF EXISTS minor_compaction")
      sql(
        "CREATE table minor_compaction (empno int, empname String, arrayInt array<int>) STORED " +
        "AS carbondata")
      sql("insert into minor_compaction select 11,'arvind',array(1,2,3)")
      sql("insert into minor_compaction select 12,'krithi',array(1,2)")
      // perform compaction operation
      sql("alter table minor_compaction compact 'minor'")
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
          CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
    }
  }

  test("Test custom compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    sql("DROP TABLE IF EXISTS custom_compaction_table")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS custom_compaction_table(
         |   ID Int,
         |   date Date,
         |   country String,
         |   name String,
         |   phonetype String,
         |   serialname String,
         |   salary Int,
         |   floatField float
         | )
         | STORED AS carbondata
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark/src/main/resources/dataSample.csv"

    // load 4 segments
    // scalastyle:off
    (1 to 4).foreach(_ => sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE custom_compaction_table
         | OPTIONS('HEADER'='true')
       """.stripMargin))
    // scalastyle:on

    sql("SHOW SEGMENTS FOR TABLE custom_compaction_table").show()

    sql("ALTER TABLE custom_compaction_table COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2)")

    sql("SHOW SEGMENTS FOR TABLE custom_compaction_table").show()
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

  }

  test("test with add segment") {
    val newSegmentPath: String = storePath + "presto_spark_db/newsegment/"
    FileFactory.getCarbonFile(newSegmentPath).delete()
    sql("drop table if exists segment_table")
    sql("create table segment_table(a string, b int, arrayInt array<int>) stored as carbondata")
    sql("insert into segment_table select 'k', 1, array(1,2,3)")
    sql("insert into segment_table select 'l', 2, array(1,2)")
    val carbonTable = CarbonEnv.getCarbonTable(None, "segment_table")(sqlContext.sparkSession)
    val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, "0")
    val schema = CarbonSchemaReader.readSchema(segmentPath).asOriginOrder()
    val writer = new CarbonWriterBuilder()
      .outputPath(newSegmentPath)
      .withCsvInput(schema)
      .writtenBy("SparkStoreCreatorForPresto")
      .build()
    writer.write(Array[String]("m", "3", "1" + "\001" + "5"))
    writer.close()
    sql(s"alter table segment_table add segment options('path'='${ newSegmentPath }', " +
        s"'format'='carbon')")
  }

  test("test with delete segment") {
    sql("drop table if exists delete_segment_table")
    sql(
      "create table delete_segment_table(a string, b int, arrayInt array<int>) stored as " +
      "carbondata")
    sql("insert into delete_segment_table select 'k',1,array(1,2,3)")
    sql("insert into delete_segment_table select 'l',2,array(1,2)")
    sql("insert into delete_segment_table select 'm',3,array(1)")
    sql("delete from table delete_segment_table where segment.id in (1)")
  }

  test("Test inverted index with update operation") {
    sql("drop table IF EXISTS inv_table")
    sql(
      "create table inv_table(name string, c_code int, arrayInt array<int>) STORED AS carbondata " +
      "tblproperties('sort_columns'='name', 'inverted_index'='name','sort_scope'='local_sort')")
    sql("insert into table inv_table select 'John',1,array(1)")
    sql("insert into table inv_table select 'John',2,array(1,2)")
    sql("insert into table inv_table select 'Neil',3,array(1,2,3)")
    sql("insert into table inv_table select 'Neil',4,array(1,2,3,4)")

    sql("update inv_table set (name) = ('Alex') where c_code = 1")
  }

  test("Test partition columns") {
    sql("drop table IF EXISTS partition_table")
    sql(
      "create table partition_table(name string, id int) PARTITIONED by (department string) " +
      "stored " +
      "as carbondata")
    sql("insert into table partition_table select 'John','1','dev'")
    sql("insert into table partition_table select 'John','4','dev'")
    sql("insert into table partition_table select 'Neil','2','test'")
    sql("insert into table partition_table select 'Neil','2','test'")

    // update
    sql("update partition_table set (name) = ('Alex') where id = 4")
    sql("update partition_table set (department) = ('Carbon-dev') where id = 4")
  }

  test("test create bloom index on table with existing data") {
    val bigFile = s"$resourcesPath/bloom_index_input_big.csv"

    val normalTable = "carbon_normal"
    val bloomSampleTable = "carbon_bloom"
    val indexName = "bloom_dm"
    createFile(bigFile, line = 50000)

    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    // load two segments
    (1 to 2).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

  }

  test("Test range columns") {
    sql("drop table IF EXISTS range_table")
    sql(
      "create table range_table(name string, id int) stored " +
      "as carbondata TBLPROPERTIES('RANGE_COLUMN' = 'name')")
    sql("insert into table range_table select 'John','1000'")
    sql("insert into table range_table select 'Alex','1001'")
    sql("insert into table range_table select 'Neil','5000'")
    sql("insert into table range_table select 'Jack','4999'")
  }

  test("Test streaming") {
    sql("drop table IF EXISTS streaming_table")
    sql(
      """
        | CREATE TABLE streaming_table(
        |    c1 string,
        |    c2 int,
        |    c3 string,
        |    c5 string
        | ) STORED AS carbondata
        | TBLPROPERTIES ('streaming' = 'true')
      """.stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO TABLE streaming_table""")
  }

  test("Test decimal unscaled converter for array") {
    sql("drop table if exists array_decimal")
    sql(
      "CREATE TABLE IF NOT EXISTS array_decimal (salary array<decimal(20,3)>) STORED AS " +
      "carbondata"
    )
    sql("insert into array_decimal select array(922.580, 3.435) ")
  }

  test("Test decimal unscaled converter for struct") {
    sql("drop table if exists struct_decimal")
    sql(
      "CREATE TABLE IF NOT EXISTS struct_decimal (salary struct<dec:decimal(20,3)> ) STORED AS " +
      "carbondata"
    )
    sql("insert into struct_decimal select named_struct('dec',922.580) ")
  }

  test("Test short vector datatype") {
    sql("drop table if exists array_short")
    sql(
      "CREATE TABLE IF NOT EXISTS array_short (salary array<short>) STORED AS " +
      "carbondata"
    )
    sql("insert into array_short select array(4352,35,3) ") // page datatype - short, adaptive
    // integral codec
  }

  test("Test int vector datatype") {
    sql("drop table if exists array_int")
    sql(
      "CREATE TABLE IF NOT EXISTS array_int (salary array<int>) STORED AS " +
      "carbondata"
    )
    sql("insert into array_int select array(21474836,21474839,23,3) ") // page datatype - int,
    // adaptive integral codec

    sql("insert into array_int select array(21474836,21474839) ") // page datatype - byte, adaptive
    // delta integral codec

  }

  test("Test long vector datatype") {
    sql("drop table if exists array_long")
    sql(
      "CREATE TABLE IF NOT EXISTS array_long (salary array<long>) STORED AS " +
      "carbondata"
    )
    // following are for adaptive integral codec
    sql("insert into array_long select array(215,23,3) ") // page datatype - short

    sql("insert into array_long select array(32800,23,3) ") // page datatype - short_int

    sql("insert into array_long select array(32800,214748364,3) ") // page datatype - int

  }

  test("Test double vector datatype") {
    sql("drop table if exists array_double")
    sql(
      "CREATE TABLE IF NOT EXISTS array_double (salary array<double>) STORED AS " +
      "carbondata"
    )
    // following are for adaptive integral codec
    sql("insert into array_double select array(2,3,4) ") // page datatype - byte

    sql("insert into array_double select array(242,35,43) ") // page datatype - short

    sql("insert into array_double select array(32799,32767) ") // page datatype - short_int

    sql("insert into array_double select array(21546546,32546546,43211564) ") // page datatype - int

    // following are for adaptive floating codec
    sql("insert into array_double select array(327.99,3.2799) ") // page datatype - short_int

    sql("insert into array_double select array(1,2345,108787.123) ") // page datatype - int

  }

  test("Test timestamp vector datatype") {
    val default_timezone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"))
    sql("drop table if exists array_timestamp")
    sql(
      "CREATE TABLE IF NOT EXISTS array_timestamp (time array<timestamp>) STORED AS " +
      "carbondata"
    )
    sql("insert into array_timestamp select array('2020-01-11 12:00:45.0','2020-01-11 12:01:45.0')")
    // page datatype - short-int, adaptive delta integral codec

    sql("insert into array_timestamp select array('2020-01-10 12:30:45.0','2015-01-11 12:01:45.0')")
    // page datatype - long, adaptive integral

    // set timezone back to default
    TimeZone.setDefault(default_timezone)
  }

  private def createFile(fileName: String, line: Int = 10000, start: Int = 0) = {
    if (!new File(fileName).exists()) {
      val write = new PrintWriter(new File(fileName))
      for (i <- start until (start + line)) {
        // scalastyle:off println
        write.println(
          s"$i,n$i,city_$i,${ Random.nextInt(80) }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }")
        // scalastyle:on println
      }
      write.close()
    }
  }

}
