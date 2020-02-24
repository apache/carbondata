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
package org.apache.carbondata.spark.testsuite.booleantype

import java.io.{File, PrintWriter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}


class BooleanDataTypesBigFileTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath

  override def beforeEach(): Unit = {
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists boolean_table")
    sql("drop table if exists boolean_table2")
    sql("drop table if exists carbon_table")
    sql("drop table if exists hive_table")
    assert(BooleanFile.deleteFile(pathOfManyDataType))
    assert(BooleanFile.deleteFile(pathOfOnlyBoolean))
  }

  val pathOfManyDataType = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBigFile.csv"
  val pathOfOnlyBoolean = s"$rootPath/integration/spark/src/test/resources/bool/supportBooleanBigFileOnlyBoolean.csv"
  val trueNum = 10000

  override def beforeAll(): Unit = {
    assert(BooleanFile.createBooleanFileWithOtherDataType(pathOfManyDataType, trueNum))
    assert(BooleanFile.createOnlyBooleanFile(pathOfOnlyBoolean, trueNum))
  }

  test("Loading table: support boolean and other data type, big file") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Row(trueNum + trueNum / 10))
  }

  test("Inserting table: support boolean and other data type, big file") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE boolean_table2(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin)

    sql("insert into boolean_table2 select * from boolean_table")

    checkAnswer(
      sql("select count(*) from boolean_table2"),
      Row(trueNum + trueNum / 10))
  }

  test("Filtering table: support boolean data type, only boolean, big file") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | booleanField BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfOnlyBoolean}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='booleanField')
           """.stripMargin)

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is not null"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is null"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = true"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField >= true"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > true"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < true"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = false"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField <= false"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > false"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < false"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (false)"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (false)"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (true,false)"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (true,false)"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField like 'f%'"),
      Row(trueNum / 10))
  }

  test("Filtering table: support boolean and other data type, big file") {

    sql(
      s"""
         | CREATE TABLE boolean_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql("select booleanField from boolean_table where intField >=1 and intField <11"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true))
    )

    checkAnswer(
      sql(s"select booleanField from boolean_table where intField >='${trueNum - 5}' and intField <=${trueNum + 1}"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false))
    )

    checkAnswer(
      sql(s"select count(*) from boolean_table where intField >='${trueNum - 5}' and doubleField <=${trueNum + 1} and booleanField=false"),
      Seq(Row(2))
    )

    checkAnswer(
      sql(s"select * from boolean_table where intField >4 and doubleField < 6.0"),
      Seq(Row(5, true, "num5", 5.0, false))
    )

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is not null"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is null"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = true"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField >= true"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > true"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < true"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = false"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField <= false"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > false"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < false"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (false)"),
      Row(trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (false)"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (true,false)"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (true,false)"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField like 'f%'"),
      Row(trueNum / 10))
  }

  test("Filtering table: support boolean and other data type, big file, load twice") {

    sql(
      s"""
         | CREATE TABLE boolean_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)
    val repeat: Int = 2
    for (i <- 0 until repeat) {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
           | INTO TABLE boolean_table
           | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin
      )
    }

    checkAnswer(
      sql("select booleanField from boolean_table where intField >=1 and intField <11"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true),
        Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true))
    )

    checkAnswer(
      sql(s"select booleanField from boolean_table where intField >='${trueNum - 5}' and intField <=${trueNum + 1}"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false),
        Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false))
    )

    checkAnswer(
      sql(s"select count(*) from boolean_table where intField >='${trueNum - 5}' and doubleField <=${trueNum + 1} and booleanField=false"),
      Seq(Row(4))
    )

    checkAnswer(
      sql(s"select * from boolean_table where intField >4 and doubleField < 6.0"),
      Seq(Row(5, true, "num5", 5.0, false), Row(5, true, "num5", 5.0, false))
    )

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Row(repeat * (trueNum + trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is not null"),
      Row(repeat * (trueNum + trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is null"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = true"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField >= true"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > true"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < true"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = false"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField <= false"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > false"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < false"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (false)"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (false)"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (true,false)"),
      Row(repeat * (trueNum + trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (true,false)"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField like 'f%'"),
      Row(repeat * (trueNum / 10)))
  }

  test("Sort_columns: support boolean and other data type, big file") {
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
         | TBLPROPERTIES('sort_columns'='booleanField')
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
         | INTO TABLE boolean_table
         | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin)

    checkAnswer(
      sql(s"select booleanField from boolean_table where intField >='${trueNum - 5}' and intField <=${trueNum + 1}"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false))
    )
  }

  test("Inserting into Hive table from carbon table: support boolean data type and other format, big file") {
    sql(
      s"""
         | CREATE TABLE carbon_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE hive_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         |  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
         | INTO TABLE carbon_table
         | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin)

    sql("insert into hive_table select * from carbon_table")

    checkAnswer(
      sql(s"select booleanField from hive_table where intField >='${trueNum - 5}' and intField <=${trueNum + 1}"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false))
    )

    checkAnswer(
      sql(s"select * from hive_table where intField >4 and doubleField < 6.0"),
      Seq(Row(5, true, "num5", 5.0, false))
    )

    checkAnswer(
      sql("select count(*) from hive_table"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from hive_table where booleanField = true"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from hive_table where booleanField = false"),
      Row(trueNum / 10))
  }

  test("Inserting into carbon table from Hive table: support boolean data type and other format, big file") {
    sql(
      s"""
         | CREATE TABLE hive_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE carbon_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
         | INTO TABLE hive_table
           """.stripMargin)

    sql("insert into carbon_table select * from hive_table")

    checkAnswer(
      sql(s"select booleanField from carbon_table where intField >='${trueNum - 5}' and intField <=${trueNum + 1}"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false))
    )

    checkAnswer(
      sql(s"select * from carbon_table where intField >4 and doubleField < 6.0"),
      Seq(Row(5, true, "num5", 5.0, false))
    )

    checkAnswer(
      sql("select count(*) from carbon_table"),
      Row(trueNum + trueNum / 10))

    checkAnswer(
      sql("select count(*) from carbon_table where booleanField = true"),
      Row(trueNum))

    checkAnswer(
      sql("select count(*) from carbon_table where booleanField = false"),
      Row(trueNum / 10))
  }

  test("Filtering table: unsafe, support boolean and other data type, big file, load twice") {
    initConf()
    sql(
      s"""
         | CREATE TABLE boolean_table(
         | intField INT,
         | booleanField BOOLEAN,
         | stringField STRING,
         | doubleField DOUBLE,
         | booleanField2 BOOLEAN
         | )
         | STORED AS carbondata
       """.stripMargin)
    val repeat: Int = 2
    for (i <- 0 until repeat) {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${pathOfManyDataType}'
           | INTO TABLE boolean_table
           | options('FILEHEADER'='intField,booleanField,stringField,doubleField,booleanField2')
           """.stripMargin
      )
    }

    checkAnswer(
      sql("select booleanField from boolean_table where intField >=1 and intField <11"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true),
        Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true), Row(true))
    )

    checkAnswer(
      sql(s"select booleanField from boolean_table where intField >='${trueNum - 5}' and intField <=${trueNum + 1}"),
      Seq(Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false),
        Row(true), Row(true), Row(true), Row(true), Row(true), Row(false), Row(false))
    )

    checkAnswer(
      sql(s"select count(*) from boolean_table where intField >='${trueNum - 5}' and doubleField <=${trueNum + 1} and booleanField=false"),
      Seq(Row(4))
    )

    checkAnswer(
      sql(s"select * from boolean_table where intField >4 and doubleField < 6.0"),
      Seq(Row(5, true, "num5", 5.0, false), Row(5, true, "num5", 5.0, false))
    )

    checkAnswer(
      sql("select count(*) from boolean_table"),
      Row(repeat * (trueNum + trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is not null"),
      Row(repeat * (trueNum + trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField is null"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = true"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField >= true"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > true"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < true"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField = false"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField <= false"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField > false"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField < false"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (false)"),
      Row(repeat * (trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (false)"),
      Row(repeat * (trueNum)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField in (true,false)"),
      Row(repeat * (trueNum + trueNum / 10)))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField not in (true,false)"),
      Row(0))

    checkAnswer(
      sql("select count(*) from boolean_table where booleanField like 'f%'"),
      Row(repeat * (trueNum / 10)))
    defaultConf()
  }

  def initConf(): Unit = {
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")
  }

  def defaultConf(): Unit = {
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        CarbonCommonConstants.ENABLE_DATA_LOADING_STATISTICS_DEFAULT)
  }
}

object BooleanFile {
  def createBooleanFileWithOtherDataType(path: String, trueLines: Int): Boolean = {
    try {
      val write = new PrintWriter(path)
      var d: Double = 0.0
      for (i <- 0 until trueLines) {
        write.println(i + "," + true + ",num" + i + "," + d + "," + false)
        d = d + 1
      }
      for (i <- 0 until trueLines / 10) {
        write.println((trueLines + i) + "," + false + ",num" + (trueLines + i) + "," + d + "," + true)
        d = d + 1
      }
      write.close()
    } catch {
      case _: Exception => assert(false)
    }
    return true
  }

  def deleteFile(path: String): Boolean = {
    try {
      val file = new File(path)
      file.delete()
    } catch {
      case _: Exception => assert(false)
    }
    return true
  }

  def createOnlyBooleanFile(path: String, num: Int): Boolean = {
    try {
      val write = new PrintWriter(path)
      for (i <- 0 until num) {
        write.println(true)
      }
      for (i <- 0 until num / 10) {
        write.println(false)
      }
      write.close()
    } catch {
      case _: Exception => assert(false)
    }
    return true
  }
}
