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

package org.apache.carbondata.sql.commands

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants

class StoredAsCarbondataSuite extends QueryTest with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS tableSize3")
  }

  override def afterEach(): Unit = {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS tableSize3")
  }

  test("CARBONDATA-2262: Support the syntax of 'STORED AS CARBONDATA', upper case") {
    sql("CREATE TABLE carbon_table(key INT, value STRING) STORED AS CARBONDATA")
    sql("INSERT INTO carbon_table VALUES (28,'Bob')")
    checkAnswer(sql("SELECT * FROM carbon_table"), Seq(Row(28, "Bob")))
  }

  test("CARBONDATA-2262: Support the syntax of 'STORED AS carbondata', low case") {
    sql("CREATE TABLE carbon_table(key INT, value STRING) STORED AS carbondata")
    sql("INSERT INTO carbon_table VALUES (28,'Bob')")
    checkAnswer(sql("SELECT * FROM carbon_table"), Seq(Row(28, "Bob")))
  }

  test("CARBONDATA-2262: Support the syntax of 'STORED AS carbondata, get data size and index size after minor compaction") {
    sql("CREATE TABLE tableSize3 (empno INT, workgroupcategory STRING, deptno INT, projectcode INT, attendance INT) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql("ALTER TABLE tableSize3 COMPACT 'minor'")
    checkExistence(sql("DESCRIBE FORMATTED tableSize3"), true, CarbonCommonConstants.TABLE_DATA_SIZE)
    checkExistence(sql("DESCRIBE FORMATTED tableSize3"), true, CarbonCommonConstants.TABLE_INDEX_SIZE)
    val res3 = sql("DESCRIBE FORMATTED tableSize3").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
        row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res3.length == 2)
    res3.foreach(row => assert(row.getString(1).trim.substring(0, 3).toDouble > 0))
  }

  test("CARBONDATA-2262: Don't Support the syntax of 'STORED AS 'carbondata''") {
    try {
      sql("CREATE TABLE carbon_table(key INT, value STRING) STORED AS 'carbondata'")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("mismatched input"))
    }
  }

  test("CARBONDATA-2262: Don't Support the syntax of 'stored by carbondata'") {
    try {
      sql("CREATE TABLE carbon_table(key INT, value STRING) STORED BY carbondata")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("mismatched input"))
    }
  }

  test("CARBONDATA-2262: Don't Support the syntax of 'STORED AS  ', null format") {
    try {
      sql("CREATE TABLE carbon_table(key INT, value STRING) STORED AS  ")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("no viable alternative at input") ||
        e.getMessage.contains("mismatched input '<EOF>' expecting "))
    }
  }

  test("CARBONDATA-2262: Don't Support the syntax of 'STORED AS carbon'") {
    try {
      sql("CREATE TABLE carbon_table(key INT, value STRING) STORED AS carbon")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("Operation not allowed: STORED AS with file format 'carbon'"))
    }
  }
}
