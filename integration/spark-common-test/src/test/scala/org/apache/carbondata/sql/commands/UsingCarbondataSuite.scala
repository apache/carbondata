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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants

class UsingCarbondataSuite extends QueryTest with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    sql("DROP TABLE IF EXISTS src_carbondata1")
    sql("DROP TABLE IF EXISTS tableSize3")
  }

  override def afterEach(): Unit = {
    sql("DROP TABLE IF EXISTS src_carbondata1")
    sql("DROP TABLE IF EXISTS tableSize3")
  }

  test("CARBONDATA-2262: test check results of table with complex data type and bucketing") {
    sql("DROP TABLE IF EXISTS create_source")
    sql("CREATE TABLE create_source(intField INT, stringField STRING, complexField ARRAY<INT>) " +
      "USING carbondata")
    sql("""INSERT INTO create_source VALUES(1,"source","1$2$3")""")
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
    res3.foreach(row => assert(row.getString(1).trim.toLong > 0))
  }

}
