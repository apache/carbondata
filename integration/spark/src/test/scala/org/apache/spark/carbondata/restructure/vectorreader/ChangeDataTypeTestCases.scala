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

package org.apache.spark.carbondata.restructure.vectorreader

import java.math.BigDecimal

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.statusmanager.SegmentStatusManager

class ChangeDataTypeTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("DROP TABLE IF EXISTS hivetable")
  }

  test("test change datatype on existing column and load data, insert into hive table") {
    def test_change_column_load_insert(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intfield BIGINT")
      sql(
        "CREATE TABLE hivetable(intField BIGINT,stringField STRING,charField STRING,timestampField "
          + "TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS PARQUET")
      sql("INSERT INTO TABLE hivetable SELECT * FROM changedatatypetest")
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_column_load_insert()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_column_load_insert()
  }

  test("test datatype change and filter") {
    def test_change_datatype_and_filter(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intfield BIGINT")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      checkAnswer(sql("SELECT charField FROM changedatatypetest WHERE intField > 99"),
        Seq(Row("abc"), Row("abc")))
      checkAnswer(sql("SELECT charField FROM changedatatypetest WHERE intField < 99"), Seq())
      checkAnswer(sql("SELECT charField FROM changedatatypetest WHERE intField = 100"),
        Seq(Row("abc"), Row("abc")))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_datatype_and_filter
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_datatype_and_filter
  }


  test("test change int datatype and load data") {
    def test_change_int_and_load(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intfield BIGINT")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      checkAnswer(sql("SELECT SUM(intField) FROM changedatatypetest"), Row(200))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_int_and_load()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_int_and_load()
  }

  test("test change int datatype of range column and load data") {
    beforeAll
    sql("CREATE TABLE changedatatypetest (ID Int, date Timestamp, country String," +
        " name String, phonetype String, serialname String, salary Int) STORED AS carbondata" +
        " TBLPROPERTIES ('range_column'='salary')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    sql("ALTER TABLE changedatatypetest CHANGE salary salary BIGINT")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    checkAnswer(sql("SELECT count(*) FROM changedatatypetest"), Row(200))
    afterAll
  }

  test("test change int datatype for bucket column and load data") {
    beforeAll
    sql("CREATE TABLE changedatatypetest (ID Int, date Timestamp, country String," +
      " name String, phonetype String, serialname String, salary Int) STORED AS carbondata" +
      " TBLPROPERTIES ('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='salary')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    sql("ALTER TABLE changedatatypetest CHANGE salary salary BIGINT")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    checkAnswer(sql("SELECT count(*) FROM changedatatypetest"), Row(200))
    afterAll
  }

  test("perform compaction after altering datatype of bucket column") {
    beforeAll
    sql("CREATE TABLE changedatatypetest (ID Int, date Timestamp, country String," +
        " name String, phonetype String, serialname String, salary Int) STORED AS carbondata" +
        " TBLPROPERTIES ('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='salary')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    sql("ALTER TABLE changedatatypetest CHANGE salary salary BIGINT")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE changedatatypetest")
    sql("ALTER TABLE changedatatypetest COMPACT 'minor'").collect()
    checkAnswer(sql("SELECT count(*) FROM changedatatypetest"), Row(400))
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME, "changedatatypetest")
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val segmentStatusManager: SegmentStatusManager =
      new SegmentStatusManager(absoluteTableIdentifier)
    val segments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala
      .map(_.getSegmentNo)
      .toList
    assert(segments.contains("0.1"))
    assert(!segments.contains("0"))
    assert(!segments.contains("1"))
    afterAll
  }

  test("test change decimal datatype and compaction") {
    def test_change_decimal_and_compaction(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE decimalField decimalField DECIMAL(9,5)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
          s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
          + s"decimalField')")
      checkAnswer(sql("SELECT decimalField FROM changedatatypetest"),
        Seq(Row(new BigDecimal("21.23").setScale(5)), Row(new BigDecimal("21.23").setScale(5))))
      sql("ALTER TABLE changedatatypetest COMPACT 'major'")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE changedatatypetest"), true, "0 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE changedatatypetest"), true, "1 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE changedatatypetest"), true, "0.1 Success")
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_decimal_and_compaction()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_decimal_and_compaction()
  }

  test("test to change int datatype to long") {
    def test_change_int_to_long(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE changedatatypetest(intField INT,stringField STRING,charField STRING," +
          "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,"
        + s"decimalField')")
      sql("ALTER TABLE changedatatypetest CHANGE intField intField LONG")
      checkAnswer(sql("SELECT intField FROM changedatatypetest LIMIT 1"), Row(100))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_int_to_long()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_int_to_long()
  }

  test("test data type change for dictionary exclude INT type column") {
    def test_change_data_type(): Unit = {
      beforeAll
    sql("drop table if exists table_sort")
    sql("CREATE TABLE table_sort (imei int,age int,mac string) STORED AS carbondata " +
        "TBLPROPERTIES('SORT_COLUMNS'='imei,age')")
    sql("insert into table_sort select 32674,32794,'MAC1'")
    sql("alter table table_sort change age age bigint")
    sql("insert into table_sort select 32675,9223372036854775807,'MAC2'")
    try {
      sqlContext.setConf("carbon.enable.vector.reader", "true")
      checkAnswer(sql("select * from table_sort"),
        Seq(Row(32674, 32794, "MAC1"), Row(32675, Long.MaxValue, "MAC2")))
    } finally {
      sqlContext.setConf("carbon.enable.vector.reader", "true")
    }
      afterAll
  }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_change_data_type()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_change_data_type()
  }

  override def afterAll {
    sqlContext.setConf("carbon.enable.vector.reader",
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("DROP TABLE IF EXISTS hivetable")
  }
}
