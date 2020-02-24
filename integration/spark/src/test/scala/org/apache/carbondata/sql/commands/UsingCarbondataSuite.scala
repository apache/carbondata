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

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants

class UsingCarbondataSuite extends QueryTest with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    sql("DROP TABLE IF EXISTS src_carbondata1")
    sql("DROP TABLE IF EXISTS src_carbondata3")
    sql("DROP TABLE IF EXISTS src_carbondata4")
    sql("DROP TABLE IF EXISTS tableSize3")
  }

  override def afterEach(): Unit = {
    sql("DROP TABLE IF EXISTS src_carbondata1")
    sql("DROP TABLE IF EXISTS src_carbondata3")
    sql("DROP TABLE IF EXISTS src_carbondata4")
    sql("DROP TABLE IF EXISTS tableSize3")
  }

  test("CARBONDATA-2262: test check results of table with complex data type and bucketing") {
    sql("DROP TABLE IF EXISTS create_source")
    sql("CREATE TABLE create_source(intField INT, stringField STRING, complexField ARRAY<INT>) USING carbondata")
    sql("INSERT INTO create_source VALUES(1,'source',array(1,2,3))")
    checkAnswer(sql("SELECT * FROM create_source"), Row(1, "source", mutable.WrappedArray.newBuilder[Int].+=(1, 2, 3)))
    sql("DROP TABLE IF EXISTS create_source")
  }

  test("CARBONDATA-2262: Support the syntax of 'USING CarbonData' whithout tableName") {
    sql("CREATE TABLE src_carbondata1(key INT, value STRING) USING carbondata")
    sql("INSERT INTO src_carbondata1 VALUES(1,'source')")
    checkAnswer(sql("SELECT * FROM src_carbondata1"), Row(1, "source"))
  }

   test("CARBONDATA-2262: Support the syntax of 'STORED AS carbondata, get data size and index size after minor compaction") {
    sql("CREATE TABLE tableSize3 (empno INT, workgroupcategory STRING, deptno INT, projectcode INT, attendance INT) USING carbondata")
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
    res3.foreach(row => assert(row.getString(1).trim.substring(0, 4).toDouble > 0))
  }

  test("CARBONDATA-2396 Support Create Table As Select with 'using carbondata'") {
    sql("CREATE TABLE src_carbondata3(key INT, value STRING) USING carbondata")
    sql("INSERT INTO src_carbondata3 VALUES(1,'source')")
    checkAnswer(sql("SELECT * FROM src_carbondata3"), Row(1, "source"))
    sql("CREATE TABLE src_carbondata4 USING carbondata as select * from src_carbondata3")
    checkAnswer(sql("SELECT * FROM src_carbondata4"), Row(1, "source"))
  }

  test("CARBONDATA-2396 Support Create Table As Select with 'USING carbondata'") {
    sql("DROP TABLE IF EXISTS src_carbondata3")
    sql("DROP TABLE IF EXISTS src_carbondata4")
    sql("CREATE TABLE src_carbondata3(key INT, value STRING) USING carbondata")
    sql("INSERT INTO src_carbondata3 VALUES(1,'source')")
    checkAnswer(sql("SELECT * FROM src_carbondata3"), Row(1, "source"))
    sql("CREATE TABLE src_carbondata4 USING carbondata as select * from src_carbondata3")
    checkAnswer(sql("SELECT * FROM src_carbondata4"), Row(1, "source"))
    sql("DROP TABLE IF EXISTS src_carbondata3")
    sql("DROP TABLE IF EXISTS src_carbondata4")
  }

  test("CARBONDATA-2396 Support Create Table As Select [IF NOT EXISTS] with 'using carbondata'") {
    sql("DROP TABLE IF EXISTS src_carbondata5")
    sql("DROP TABLE IF EXISTS src_carbondata6")
    sql("CREATE TABLE src_carbondata5(key INT, value STRING) USING carbondata")
    sql("INSERT INTO src_carbondata5 VALUES(1,'source')")
    checkAnswer(sql("SELECT * FROM src_carbondata5"), Row(1, "source"))
    sql(
      "CREATE TABLE IF NOT EXISTS src_carbondata6 USING carbondata as select * from " +
      "src_carbondata5")
    checkAnswer(sql("SELECT * FROM src_carbondata6"), Row(1, "source"))
    sql("DROP TABLE IF EXISTS src_carbondata5")
    sql("DROP TABLE IF EXISTS src_carbondata6")
  }

  test("CARBONDATA-2396 Support Create Table As Select with 'using carbondata' with Table properties") {
    sql("DROP TABLE IF EXISTS src_carbondata5")
    sql("DROP TABLE IF EXISTS src_carbondata6")
    sql("CREATE TABLE src_carbondata5(key INT, value STRING) USING carbondata")
    sql("INSERT INTO src_carbondata5 VALUES(1,'source')")
    checkAnswer(sql("SELECT * FROM src_carbondata5"), Row(1, "source"))
    sql("CREATE TABLE src_carbondata6  USING carbondata options('table_blocksize'='10'," +
      "'sort_scope'='local_sort') as select * from src_carbondata5")
    val result = sql("describe FORMATTED src_carbondata6")
    checkExistence(result, true, "Table Block Size")
    checkExistence(result, true, "10 MB")
    checkAnswer(sql("SELECT * FROM src_carbondata6"), Row(1, "source"))
    sql("DROP TABLE IF EXISTS src_carbondata5")
    sql("DROP TABLE IF EXISTS src_carbondata6")
  }

  test("CARBONDATA-2396 Support Create Table As Select with 'using carbondata' with Columns") {
    sql("DROP TABLE IF EXISTS src_carbondata5")
    sql("DROP TABLE IF EXISTS src_carbondata6")
    sql("CREATE TABLE src_carbondata5(key INT, value STRING) USING carbondata")
    sql("INSERT INTO src_carbondata5 VALUES(1,'source')")
    checkAnswer(sql("SELECT * FROM src_carbondata5"), Row(1, "source"))
    val exception = intercept[AnalysisException](
      sql(
        "CREATE TABLE src_carbondata6(name String) USING carbondata as select * from " +
        "src_carbondata5"))
    assert(exception.getMessage
      .contains(
        "Operation not allowed: Schema may not be specified in a Create Table As Select (CTAS) " +
        "statement"))
    sql("DROP TABLE IF EXISTS src_carbondata5")
    sql("DROP TABLE IF EXISTS src_carbondata6")
  }

}
