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

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * test class for describe table .
  */
class TestDescribeTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS Desc1")
    sql("DROP TABLE IF EXISTS Desc2")
    sql("DROP TABLE IF EXISTS Desc3")
    sql("DROP TABLE IF EXISTS Desc4")
    sql(
      "CREATE TABLE Desc1(Dec1Col1 String, Dec1Col2 String, Dec1Col3 int, Dec1Col4 double) stored" +
      " by 'carbondata'")
    sql("DESC Desc1")
    sql("DROP TABLE Desc1")
    sql(
      "CREATE TABLE Desc1(Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) " +
      "stored by 'carbondata'")
    sql(
      "CREATE TABLE Desc2(Dec2Col1 BigInt, Dec2Col2 String, Dec2Col3 Bigint, Dec2Col4 Decimal) " +
      "stored by 'carbondata'")
  }

  test("test describe table") {
    checkAnswer(sql("DESC Desc1"), sql("DESC Desc2"))
  }

  test("test describe formatted table") {
    checkExistence(sql("DESC FORMATTED Desc1"), true,
      "Table Block Size :")
  }

  test("test describe temporary table") {
    val tempLocation = Files.createTempDirectory("Desc3")
    try {
      sql(
        s"""
           | CREATE TEMPORARY TABLE Desc3
           | (Desc3Col1 int,Desc3Col2 string)
           | USING PARQUET
           | OPTIONS (path '$tempLocation')
      """.stripMargin)
      sql("CREATE temporary TABLE Desc3(Desc3Col1 int,Desc3Col2 string) using parquet")
      checkAnswer(sql("DESC Desc3"),
        Seq(Row("Desc3Col1", "int", null), Row("Desc3Col2", "string", null)))
      assert(true)
    } catch {
      case e:Exception =>
        System.out.println("Desc3 errormessage: "+e.getMessage)
        assert(false)
    } finally {
      sql("DROP TABLE if exists Desc3")
      FileUtils.deleteQuietly(tempLocation.toFile)

    }
  }

  test("test describe carbon table when a temp table of same name exists") {
    val tempLocation = Files.createTempDirectory("Desc4")
    try {
      sql(s"CREATE DATABASE if not exists temptablecheckDB")
      sql("USE temptablecheckDB")
      sql(
        s"""
           | CREATE TEMPORARY TABLE Desc4
           | (Desc3Col1 int,Desc3Col2 string)
           | USING PARQUET
           | OPTIONS (path '$tempLocation')
      """.stripMargin)
      sql("CREATE temporary TABLE Desc4(Desc3Col1 int,Desc3Col2 string) using parquet")
      sql(
        "CREATE TABLE Desc4(Dec1Col1 String, Dec1Col2 String, Dec1Col3 int, Dec1Col4 " +
        "double) stored by 'carbondata'")
      sql("DESC Desc4").show(200,false)
      checkAnswer(sql("DESC Desc4"),
        Seq(Row("Desc3Col1", "int", null), Row("Desc3Col2", "string", null)))
      sql("DESC temptablecheckDB.Desc4").show(200,false)
      checkAnswer(sql("DESC temptablecheckDB.Desc4"), Seq(Row("Dec1Col1", "string", null),
        Row("Dec1Col2", "string", null),
        Row("Dec1Col3", "int", null),
        Row("Dec1Col4", "double", null)))
    } catch {
      case e:Exception =>
        System.out.println("Desc4 errormessage: "+e.getMessage)
        assert(false)
    } finally {
      sql("USE default")
      FileUtils.deleteQuietly(tempLocation.toFile)
    }
  }

  override def afterAll: Unit = {
    sql("DROP TABLE Desc1")
    sql("DROP TABLE Desc2")
    sql("DROP TABLE if exists Desc3")
    sql("DROP TABLE if exists Desc4")
    sql("DROP DATABASE if exists temptablecheckDB cascade")
  }

}
