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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class DropColumnTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("DROP TABLE IF EXISTS hivetable")
  }

  test("test drop column and insert into hive table") {
    def test_drop_and_insert() = {
      beforeAll
      sql(
        "CREATE TABLE dropcolumntest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest DROP COLUMNS(charField)")
      sql(
        "CREATE TABLE hivetable(intField INT,stringField STRING,timestampField TIMESTAMP," +
        "decimalField DECIMAL(6,2)) STORED AS PARQUET")
      sql("INSERT INTO TABLE hivetable SELECT * FROM dropcolumntest")
      checkAnswer(sql("SELECT * FROM hivetable"), sql("SELECT * FROM dropcolumntest"))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_drop_and_insert()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_drop_and_insert()
  }

  test("test drop column and load data") {
    def test_drop_and_load() = {
      beforeAll
      sql(
        "CREATE TABLE dropcolumntest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest DROP COLUMNS(charField)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField')")
      checkAnswer(sql("SELECT count(*) FROM dropcolumntest"), Row(2))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_drop_and_load
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_drop_and_load

  }

  test("test drop column and compaction") {
    def test_drop_and_compaction() = {
      beforeAll
      sql(
        "CREATE TABLE dropcolumntest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest DROP COLUMNS(charField)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest COMPACT 'major'")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE dropcolumntest"), true, "0 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE dropcolumntest"), true, "1 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE dropcolumntest"), true, "0.1 Success")
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_drop_and_compaction()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_drop_and_compaction()
  }

  test("test dropping of complex column should throw exception") {
    sql("drop table if exists maintbl")
    sql("create table maintbl (a string, b string, c struct<si:int>) STORED AS carbondata")
    assert(intercept[ProcessMetaDataException] {
      sql("alter table maintbl drop columns(b,c)").show
    }.getMessage.contains("Complex column cannot be dropped"))
    sql("drop table if exists maintbl")
  }

  override def afterAll {
    sqlContext.setConf(
      "carbon.enable.vector.reader", CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("DROP TABLE IF EXISTS hivetable")
  }
}
