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

package org.apache.carbondata.spark.testsuite.createTable

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonSessionCatalog
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{AnalysisException, CarbonEnv, CarbonSession}
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat

class TestCreateHiveTableWithCarbonDS extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS source")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS source")
  }

  test("test create table and verify the hive table correctness with stored by") {
    sql("DROP TABLE IF EXISTS source")
    sql(
      s"""
         |CREATE TABLE source (key INT, value string, col1 double)
         |STORED BY 'carbondata'
     """.stripMargin)

    verifyTable

    sql("DROP TABLE IF EXISTS source")
  }

  private def verifyTable = {
    val table = sqlContext.sparkSession.asInstanceOf[CarbonSession].sessionState.catalog.asInstanceOf[CarbonSessionCatalog].getClient().getTable("default", "source")
    assert(table.schema.fields.length == 3)
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      assert(table.storage.locationUri.get.equals(new Path(s"file:$storeLocation/source").toUri))
    }
    assert(table.storage.inputFormat.get.equals(classOf[CarbonFileInputFormat[_]].getName))
  }

  test("test create table and verify the hive table correctness with using carbondata") {
    sql("DROP TABLE IF EXISTS source")
    sql(
      s"""
         |CREATE TABLE source (key INT, value string, col1 double)
         |using carbondata
     """.stripMargin)

    verifyTable


    sql("DROP TABLE IF EXISTS source")
  }

  test("test create table and verify the hive table correctness with using carbon") {
    sql("DROP TABLE IF EXISTS source")
    sql(
      s"""
         |CREATE TABLE source (key INT, value string, col1 double)
         |using carbon
     """.stripMargin)

    verifyTable


    sql("DROP TABLE IF EXISTS source")
  }

}
