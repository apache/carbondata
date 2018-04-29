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

package org.apache.carbondata.datamap.bloom

import java.io.{File, PrintWriter}
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class BloomCoarseGrainDataMapSuite extends QueryTest with BeforeAndAfterAll {
  val inputFile = s"$resourcesPath/bloom_datamap_input.csv"
  val normalTable = "carbon_normal"
  val bloomDMSampleTable = "carbon_bloom"
  val dataMapName = "bloom_dm"
  val lineNum = 500000

  override protected def beforeAll(): Unit = {
    createFile(inputFile, line = lineNum, start = 0)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test bloom datamap") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
       """.stripMargin)

    sql(s"show datamap on table $bloomDMSampleTable").show(false)
    sql(s"select * from $bloomDMSampleTable where city = 'city_5'").show(false)
    sql(s"select * from $bloomDMSampleTable limit 5").show(false)

    checkExistence(sql(s"show datamap on table $bloomDMSampleTable"), true, dataMapName)
    checkAnswer(sql(s"select * from $bloomDMSampleTable where id = 1"),
      sql(s"select * from $normalTable where id = 1"))
    checkAnswer(sql(s"select * from $bloomDMSampleTable where id = 999"),
      sql(s"select * from $normalTable where id = 999"))
    checkAnswer(sql(s"select * from $bloomDMSampleTable where city = 'city_1'"),
      sql(s"select * from $normalTable where city = 'city_1'"))
    checkAnswer(sql(s"select * from $bloomDMSampleTable where city = 'city_999'"),
      sql(s"select * from $normalTable where city = 'city_999'"))
    checkAnswer(sql(s"select count(distinct id), count(distinct name), count(distinct city)," +
                    s" count(distinct s1), count(distinct s2) from $bloomDMSampleTable"),
      sql(s"select count(distinct id), count(distinct name), count(distinct city)," +
          s" count(distinct s1), count(distinct s2) from $normalTable"))
    checkAnswer(sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
                    s" from $bloomDMSampleTable"),
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $normalTable"))
  }

  // todo: will add more tests on bloom datamap, such as exception, delete datamap, show profiler

  override protected def afterAll(): Unit = {
    deleteFile(inputFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
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
