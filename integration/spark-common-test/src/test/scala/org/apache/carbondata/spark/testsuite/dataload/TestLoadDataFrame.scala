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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

class TestLoadDataFrame extends QueryTest with BeforeAndAfterAll {
  var df: DataFrame = _

  def buildTestData() = {
    import sqlContext.implicits._
    df = sqlContext.sparkContext.parallelize(1 to 1000)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "c3")
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS carbon1")
    sql("DROP TABLE IF EXISTS carbon2")
    sql("DROP TABLE IF EXISTS carbon3")
  }



  override def beforeAll {
    dropTable
    buildTestData
  }

  test("test load dataframe with saving compressed csv files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon1 where c3 > 500"), Row(500)
    )
  }

  test("test load dataframe with saving csv uncompressed files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .option("tempCSV", "true")
      .option("compress", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon2 where c3 > 500"), Row(500)
    )
  }

  test("test load dataframe without saving csv files") {
    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon3")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
    checkAnswer(
      sql("select count(*) from carbon3 where c3 > 500"), Row(500)
    )
  }

  override def afterAll {
    dropTable
  }
}

