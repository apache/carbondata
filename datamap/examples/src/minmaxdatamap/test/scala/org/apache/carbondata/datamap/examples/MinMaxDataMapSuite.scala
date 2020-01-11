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
package org.apache.carbondata.datamap.examples

import java.io.{File, PrintWriter}
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MinMaxDataMapSuite extends QueryTest with BeforeAndAfterAll {
  val inputFile = s"$resourcesPath/minmax_datamap_input.csv"
  val normalTable = "carbonNormal"
  val minMaxDMSampleTable = "carbonMinMax"
  val dataMapName = "minmax_dm"
  val lineNum = 500000

  override protected def beforeAll(): Unit = {
    createFile(inputFile, line = lineNum, start = 0)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $minMaxDMSampleTable")
  }
  
  test("test minmax datamap") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
        | CREATE TABLE $minMaxDMSampleTable(id INT, name STRING, city STRING, age INT,
        | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
        | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
        |  """.stripMargin)
    sql(
      s"""
        | CREATE DATAMAP $dataMapName ON TABLE $minMaxDMSampleTable
        | USING '${classOf[MinMaxIndexDataMapFactory].getName}'
      """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $minMaxDMSampleTable
         | OPTIONS('header'='false')
       """.stripMargin)

    sql(s"show datamap on table $minMaxDMSampleTable").show(false)
    // not that the table will use default dimension as sort_columns, so for the following cases,
    // the pruning result will differ.
    // 1 blocklet
    checkAnswer(sql(s"select * from $minMaxDMSampleTable where id = 1"),
      sql(s"select * from $normalTable where id = 1"))
    // 6 blocklet
    checkAnswer(sql(s"select * from $minMaxDMSampleTable where id = 999"),
      sql(s"select * from $normalTable where id = 999"))
    // 1 blocklet
    checkAnswer(sql(s"select * from $minMaxDMSampleTable where city = 'city_1'"),
      sql(s"select * from $normalTable where city = 'city_1'"))
    // 1 blocklet
    checkAnswer(sql(s"select * from $minMaxDMSampleTable where city = 'city_999'"),
      sql(s"select * from $normalTable where city = 'city_999'"))
    // 6 blocklet
    checkAnswer(sql(s"select count(distinct id), count(distinct name), count(distinct city)," +
                    s" count(distinct s1), count(distinct s2) from $minMaxDMSampleTable"),
      sql(s"select count(distinct id), count(distinct name), count(distinct city)," +
          s" count(distinct s1), count(distinct s2) from $normalTable"))
    // 6 blocklet
    checkAnswer(sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
                    s" from $minMaxDMSampleTable"),
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $normalTable"))
  }

  override protected def afterAll(): Unit = {
    deleteFile(inputFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $minMaxDMSampleTable")
  }

  private def createFile(fileName: String, line: Int = 10000, start: Int = 0) = {
    if (!new File(fileName).exists()) {
      val write = new PrintWriter(new File(fileName))
      for (i <- start until (start + line)) {
        write.println(
          s"$i,n$i,city_$i,${ Random.nextInt(80) }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }," +
          s"${ UUID.randomUUID().toString },${ UUID.randomUUID().toString }")
      }
      write.close()
    }
  }

  private def deleteFile(fileName: String): Unit = {
    val file = new File(fileName)
    if (file.exists()) {
      file.delete()
    }
  }
}