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
package org.apache.carbondata.spark.testsuite.describeTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
  * test class for describe table .
  */
class TestDescribeTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS Desc1")
    sql("DROP TABLE IF EXISTS Desc2")
    sql("drop table if exists a")
    sql("CREATE TABLE Desc1(Dec1Col1 String, Dec1Col2 String, Dec1Col3 int, Dec1Col4 double) STORED AS carbondata")
    sql("DESC Desc1")
    sql("DROP TABLE Desc1")
    sql("CREATE TABLE Desc1(Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) STORED AS carbondata")
    sql("CREATE TABLE Desc2(Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) STORED AS carbondata")
  }

  test("test describe table") {
    checkAnswer(sql("DESC Desc1"), Seq(Row("dec2col1","bigint",null),
      Row("dec2col2","string",null),
      Row("dec2col3","bigint",null),
      Row("dec2col4","decimal(10,0)",null)))
  }

  test("test describe formatted table") {
    checkExistence(sql("DESC FORMATTED Desc1"), true, "Table Block Size")
  }

  test("test describe formatted for partition table") {
    sql("create table a(a string) partitioned by (b int) STORED AS carbondata")
    sql("insert into a values('a',1)")
    sql("insert into a values('a',2)")
    val desc = sql("describe formatted a").collect()
    assert(desc(desc.indexWhere(_.get(0).toString.contains("#Partition")) + 2).get(0).toString.contains("b"))
    val descPar = sql("describe formatted a partition(b=1)").collect
    descPar.find(_.get(0).toString.contains("Partition Value:")) match {
      case Some(row) => assert(row.get(1).toString.contains("1"))
      case None => fail("Partition Value not found in describe formatted")
    }
    descPar.find(_.get(0).toString.contains("Location:")) match {
      case Some(row) => assert(row.get(1).toString.contains("target/warehouse/a/b=1"))
      case None => fail("Partition Location not found in describe formatted")
    }
    assert(descPar.exists(_.toString().contains("Partition Parameters:")))
  }

  test(testName = "Compressor Type update from carbon properties") {
    sql("drop table if exists b")
    sql(sqlText = "create table b(a int,b string) STORED AS carbondata")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR, "gzip")
    val result = sql(sqlText = "desc formatted b").collect()
    assert(result.filter(row => row.getString(0).contains("Data File Compressor")).head.getString
    (1).equalsIgnoreCase("gzip"))
  }

  override def afterAll: Unit = {
    sql("DROP TABLE Desc1")
    sql("DROP TABLE Desc2")
    sql("drop table if exists a")
    sql("drop table if exists b")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.COMPRESSOR,
      CarbonCommonConstants.DEFAULT_COMPRESSOR)
  }

}
