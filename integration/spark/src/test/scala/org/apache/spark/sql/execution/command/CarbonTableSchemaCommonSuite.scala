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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.exception.ProcessMetaDataException

class CarbonTableSchemaCommonSuite extends QueryTest with BeforeAndAfterAll {

  test("Creating table: Duplicate dimensions found with name, it should throw AnalysisException") {
    sql("DROP TABLE IF EXISTS carbon_table")
    try {
      sql(
        s"""
           | CREATE TABLE carbon_table(
           | BB INT, bb char(10)
           | )
           | STORED AS carbondata
       """.stripMargin)
      Assert.assertTrue(false)
    } catch {
      case _: AnalysisException => Assert.assertTrue(true)
      case _: Exception => Assert.assertTrue(false)
    } finally {
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("Altering table: Duplicate column found with name, it should throw RuntimeException") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql(
      s"""
         | CREATE TABLE if not exists carbon_table(
         | BB INT, cc char(10)
         | )
         | STORED AS carbondata
       """.stripMargin)

    val ex = intercept[ProcessMetaDataException] {
      sql(
        s"""
           | alter TABLE carbon_table add columns(
           | bb char(10)
            )
       """.stripMargin)
    }
    sql("DROP TABLE IF EXISTS carbon_table")
  }

}
