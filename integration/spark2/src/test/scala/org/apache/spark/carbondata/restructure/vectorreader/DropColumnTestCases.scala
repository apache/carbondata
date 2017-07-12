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

import java.math.{BigDecimal, RoundingMode}

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties

class DropColumnTestCases extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("drop table if exists hivetable")
  }

  test("test drop column and insert into hive table") {
    beforeAll
    sql(
      "CREATE TABLE dropcolumntest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table dropcolumntest drop columns(charField)")
    sql(
      "CREATE TABLE hivetable(intField int,stringField string,timestampField timestamp," +
      "decimalField decimal(6,2)) stored as parquet")
    sql("insert into table hivetable select * from dropcolumntest")
    checkAnswer(sql("select * from hivetable"), sql("select * from dropcolumntest"))
    afterAll
  }

  test("test drop column and load data") {
    beforeAll
    sql(
      "CREATE TABLE dropcolumntest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table dropcolumntest drop columns(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    checkAnswer(sql("select count(*) from dropcolumntest"), Row(2))
    afterAll
  }

  test("test drop column and compaction") {
    beforeAll
    sql(
      "CREATE TABLE dropcolumntest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table dropcolumntest drop columns(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    sql("alter table dropcolumntest compact 'major'")
    checkExistence(sql("show segments for table dropcolumntest"), true, "0Compacted")
    checkExistence(sql("show segments for table dropcolumntest"), true, "1Compacted")
    checkExistence(sql("show segments for table dropcolumntest"), true, "0.1Success")
    afterAll
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("drop table if exists hivetable")
    sqlContext.setConf("carbon.enable.vector.reader", "false")
  }
}
