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

package org.apache.spark.sql

import java.util.Date

import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.scalatest.BeforeAndAfterAll

class GetDataSizeAndIndexSizeTest extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS tableSize1")
    sql("DROP TABLE IF EXISTS tableSize2")
    sql("DROP TABLE IF EXISTS tableSize3")
    sql("DROP TABLE IF EXISTS tableSize4")
    sql("DROP TABLE IF EXISTS tableSize5")
    sql("DROP TABLE IF EXISTS tableSize6")
    sql("DROP TABLE IF EXISTS tableSize7")
    sql("DROP TABLE IF EXISTS tableSize8")
    sql("DROP TABLE IF EXISTS tableSize9")
    sql("DROP TABLE IF EXISTS tableSize10")
    sql("DROP TABLE IF EXISTS tableSize11")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS tableSize1")
    sql("DROP TABLE IF EXISTS tableSize2")
    sql("DROP TABLE IF EXISTS tableSize3")
    sql("DROP TABLE IF EXISTS tableSize4")
    sql("DROP TABLE IF EXISTS tableSize5")
    sql("DROP TABLE IF EXISTS tableSize6")
    sql("DROP TABLE IF EXISTS tableSize7")
    sql("DROP TABLE IF EXISTS tableSize8")
    sql("DROP TABLE IF EXISTS tableSize9")
    sql("DROP TABLE IF EXISTS tableSize10")
    sql("DROP TABLE IF EXISTS tableSize11")
  }

  test("get data size and index size after load data") {
    sql("CREATE TABLE tableSize1 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize1 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    checkExistence(sql("DESCRIBE FORMATTED tableSize1"), true, CarbonCommonConstants.TABLE_DATA_SIZE)
    checkExistence(sql("DESCRIBE FORMATTED tableSize1"), true, CarbonCommonConstants.TABLE_INDEX_SIZE)
    val res1 = sql("DESCRIBE FORMATTED tableSize1").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
      row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res1.length == 2)
    res1.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
  }

  test("get data size and index size after major compaction") {
    sql("CREATE TABLE tableSize2 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize2 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize2 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql("ALTER TABLE tableSize2 compact 'major'")
    checkExistence(sql("DESCRIBE FORMATTED tableSize2"), true, CarbonCommonConstants.TABLE_DATA_SIZE)
    checkExistence(sql("DESCRIBE FORMATTED tableSize2"), true, CarbonCommonConstants.TABLE_INDEX_SIZE)
    val res2 = sql("DESCRIBE FORMATTED tableSize2").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
        row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res2.length == 2)
    res2.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
  }

  test("get data size and index size after minor compaction") {
    sql("CREATE TABLE tableSize3 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize3 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql("ALTER TABLE tableSize3 compact 'minor'")
    checkExistence(sql("DESCRIBE FORMATTED tableSize3"), true, CarbonCommonConstants.TABLE_DATA_SIZE)
    checkExistence(sql("DESCRIBE FORMATTED tableSize3"), true, CarbonCommonConstants.TABLE_INDEX_SIZE)
    val res3 = sql("DESCRIBE FORMATTED tableSize3").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
        row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res3.length == 2)
    res3.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
  }

  test("get data size and index size after insert into") {
    sql("CREATE TABLE tableSize4 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize4 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql("CREATE TABLE tableSize5 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql("INSERT INTO TABLE tableSize5 SELECT * FROM tableSize4")
    checkExistence(sql("DESCRIBE FORMATTED tableSize5"), true, CarbonCommonConstants.TABLE_DATA_SIZE)
    checkExistence(sql("DESCRIBE FORMATTED tableSize5"), true, CarbonCommonConstants.TABLE_INDEX_SIZE)
    val res4 = sql("DESCRIBE FORMATTED tableSize5").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
        row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res4.length == 2)
    res4.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
  }

  test("get data size and index size after insert overwrite") {
    sql("CREATE TABLE tableSize6 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize6 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
    sql("CREATE TABLE tableSize7 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql("INSERT OVERWRITE TABLE tableSize7 SELECT * FROM tableSize6")
    checkExistence(sql("DESCRIBE FORMATTED tableSize7"), true, CarbonCommonConstants.TABLE_DATA_SIZE)
    checkExistence(sql("DESCRIBE FORMATTED tableSize7"), true, CarbonCommonConstants.TABLE_INDEX_SIZE)
    val res5 = sql("DESCRIBE FORMATTED tableSize7").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
        row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res5.length == 2)
    res5.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
  }

  test("get data size and index size for empty table") {
    sql("CREATE TABLE tableSize8 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    val res6 = sql("DESCRIBE FORMATTED tableSize8").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
        row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res6.length == 2)
    res6.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble == 0))
  }

  test("get last update time for empty table") {
    sql("CREATE TABLE tableSize9 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    val res7 = sql("DESCRIBE FORMATTED tableSize9").collect()
      .filter(row => row.getString(0).contains("Last Update"))
    assert(res7.length == 1)
  }

  test("get last update time for unempty table") {
    sql("CREATE TABLE tableSize10 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize10 OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")

    val res8 = sql("DESCRIBE FORMATTED tableSize10").collect()
      .filter(row => row.getString(0).contains("Last Update"))
    assert(res8.length == 1)
  }

  test("index and datasize for update scenario") {
    sql(
      "CREATE TABLE tableSize11 (empno int, workgroupcategory string, deptno int, projectcode " +
      "int, attendance int) STORED AS carbondata")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE tableSize11 OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""".stripMargin)
    val res9 = sql("DESCRIBE FORMATTED tableSize11").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
                     row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res9.length == 2)
    res9.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
    sql("update tableSize11 set (empno) = (234)").show()
    val res10 = sql("DESCRIBE FORMATTED tableSize11").collect()
      .filter(row => row.getString(0).contains(CarbonCommonConstants.TABLE_DATA_SIZE) ||
                     row.getString(0).contains(CarbonCommonConstants.TABLE_INDEX_SIZE))
    assert(res10.length == 2)
    res10.foreach(row => assert(row.getString(1).trim.substring(0, 2).toDouble > 0))
  }

}
