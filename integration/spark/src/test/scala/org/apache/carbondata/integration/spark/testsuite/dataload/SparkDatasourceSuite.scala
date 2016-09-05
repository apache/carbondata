/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File

import org.apache.spark.sql.{Row, DataFrame, SaveMode}
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class SparkDatasourceSuite extends QueryTest with BeforeAndAfterAll {

  var currentDirectory: String = _
  var df: DataFrame = _

  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbon1")

    currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
    import implicits._
    df = sc.parallelize(1 to 1000)
        .map(x => ("a", "b", x))
        .toDF("c1", "c2", "c3")
  }

  test("read and write using CarbonContext") {
    // save dataframe to carbon file
    df.write
        .format("carbondata")
        .option("tableName", "carbon1")
        .mode(SaveMode.Overwrite)
        .save()

    val in = read
        .format("carbondata")
        .option("tableName", "carbon1")
        .load()

    assert(in.where("c3 > 500").count() == 500)
    sql("DROP TABLE IF EXISTS carbon1")
  }

  test("saveAsCarbon API") {
    import org.apache.carbondata.spark._
    df.saveAsCarbonFile(Map("tableName" -> "carbon2"))

    checkAnswer(sql("SELECT count(*) FROM carbon2 WHERE c3 > 100"), Seq(Row(900)))
    sql("DROP TABLE IF EXISTS carbon2")
  }

  test("saveAsCarbon API using compression") {
    import org.apache.carbondata.spark._
    df.saveAsCarbonFile(Map("tableName" -> "carbon2", "compress" -> "true"))

    checkAnswer(sql("SELECT count(*) FROM carbon2 WHERE c3 > 100"), Seq(Row(900)))
    sql("DROP TABLE IF EXISTS carbon2")
  }

  override def afterAll {

  }
}
