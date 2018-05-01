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

import org.apache.spark.sql.{CarbonSession, DataFrame}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties

class BloomCoarseGrainDataMapSuite extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val bigFile = s"$resourcesPath/bloom_datamap_input_big.csv"
  val smallFile = s"$resourcesPath/bloom_datamap_input_small.csv"
  val normalTable = "carbon_normal"
  val bloomDMSampleTable = "carbon_bloom"
  val dataMapName = "bloom_dm"

  override protected def beforeAll(): Unit = {
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    createFile(bigFile, line = 500000)
    createFile(smallFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  private def checkSqlHitDataMap(sqlText: String, dataMapName: String, shouldHit: Boolean): DataFrame = {
    if (shouldHit) {
      assert(sqlContext.sparkSession.asInstanceOf[CarbonSession].isDataMapHit(sqlText, dataMapName))
    } else {
      assert(!sqlContext.sparkSession.asInstanceOf[CarbonSession].isDataMapHit(sqlText, dataMapName))
    }
    sql(sqlText)
  }

  private def checkQuery(dataMapName: String, shouldHit: Boolean = true) = {
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id = 1", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 1"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id = 999", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 999"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_1'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999'"))
     checkAnswer(
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $bloomDMSampleTable"),
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $normalTable"))
  }

  test("test create bloom datamap on table with existing data") {
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

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    // load two segments
    (1 to 2).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    checkQuery(dataMapName)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test create bloom datamap and REBUILD DATAMAP") {
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

    // load two segments
    (1 to 2).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomDMSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    checkQuery(dataMapName)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test create bloom datamap with DEFERRED REBUILD, query hit datamap") {
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
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
       """.stripMargin)

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | WITH DEFERRED REBUILD
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    var map = DataMapStatusManager.readDataMapStatusMap()
    assert(!map.get(dataMapName).isEnabled)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    map = DataMapStatusManager.readDataMapStatusMap()
    assert(!map.get(dataMapName).isEnabled)

    // once we rebuild, it should be enabled
    sql(s"REBUILD DATAMAP $dataMapName ON TABLE $bloomDMSampleTable")
    map = DataMapStatusManager.readDataMapStatusMap()
    assert(map.get(dataMapName).isEnabled)

    sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable").show(false)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)
    checkQuery(dataMapName)

    // once we load again, datamap should be disabled, since it is lazy
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)
    map = DataMapStatusManager.readDataMapStatusMap()
    assert(!map.get(dataMapName).isEnabled)
    checkQuery(dataMapName, shouldHit = false)

    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  test("test create bloom datamap with DEFERRED REBUILD, query not hit datamap") {
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
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
       """.stripMargin)

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | WITH DEFERRED REBUILD
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    checkExistence(sql(s"SHOW DATAMAP ON TABLE $bloomDMSampleTable"), true, dataMapName)

    // datamap is not loaded, so it should not hit
    checkQuery(dataMapName, shouldHit = false)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  override protected def afterAll(): Unit = {
    deleteFile(bigFile)
    deleteFile(smallFile)
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
