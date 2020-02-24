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
package org.apache.carbondata.spark.testsuite.secondaryindex

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Secondary index refresh and registration to the main table
 */
class TestRegisterIndexCarbonTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop database if exists carbon cascade")
  }

  def restoreData(dblocation: String, tableName: String) = {
    val destination = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val source = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
      FileUtils.deleteDirectory(new File(source))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data restore failed.")
    } finally {

    }
  }
  def backUpData(dblocation: String, tableName: String) = {
    val source = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val destination = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data backup failed.")
    }
  }
  test("register tables test") {
    val location = TestQueryExecutor.warehouse +
                           CarbonCommonConstants.FILE_SEPARATOR + "dbName"
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '${location}'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("create index index_on_c3 on table carbontable (c3, c5) AS 'carbondata'")
    backUpData(location, "carbontable")
    backUpData(location, "index_on_c3")
    sql("drop table carbontable")
    restoreData(location, "carbontable")
    restoreData(location, "index_on_c3")
    sql("refresh table carbontable")
    sql("refresh table index_on_c3")
    checkAnswer(sql("select count(*) from carbontable"), Row(1))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
    sql("REGISTER INDEX TABLE index_on_c3 ON carbontable")
    assert(sql("show indexes on carbontable").collect().nonEmpty)
  }
  override def afterAll {
    sql("drop database if exists carbon cascade")
    sql("use default")
  }
}
