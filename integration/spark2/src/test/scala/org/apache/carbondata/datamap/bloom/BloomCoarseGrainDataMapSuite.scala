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

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
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
    createFile(bigFile, line = 50000)
    createFile(smallFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
  }

  private def checkSqlHitDataMap(sqlText: String, dataMapName: String, shouldHit: Boolean): DataFrame = {
    // ignore checking datamap hit, because bloom bloom datamap may be skipped if
    // default blocklet datamap pruned all the blocklets.
    // We cannot tell whether the index datamap will be hit from the query.
    sql(sqlText)
  }

  private def checkQuery(dataMapName: String, shouldHit: Boolean = true) = {
    /**
     * queries that use equal operator
     */
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
    // query with two index_columns
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id = 1 and city='city_1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 1 and city='city_1'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id = 999 and city='city_999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 999 and city='city_999'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_1' and id = 0", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_1' and id = 0"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_999' and name='n999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999' and name='n999'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city = 'city_999' and name='n1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999' and name='n1'"))

    /**
     * queries that use in operator
     */
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id in (1)", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (1)"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id in (999)", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (999)"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city in( 'city_1')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in( 'city_1')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city in ('city_999')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_999')"))
    // query with two index_columns
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id in (1) and city in ('city_1')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (1) and city in ('city_1')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where id in (999) and city in ('city_999')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (999) and city in ('city_999')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city in ('city_1') and id in (0)", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_1') and id in (0)"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city in ('city_999') and name in ('n999')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_999') and name in ('n999')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomDMSampleTable where city in ('city_999') and name in ('n1')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_999') and name in ('n1')"))

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

  test("test bloom datamap: multiple datamaps with each on one column vs one datamap on multiple columns") {
    val iterations = 1
    // 500000 lines will result to 3 blocklets and bloomfilter datamap will prune 2 blocklets.
    val datamap11 = "datamap11"
    val datamap12 = "datamap12"
    val datamap13 = "datamap13"
    val datamap2 = "datamap2"

    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
    // create a table and 3 bloom datamaps on it, each datamap contains one index column
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'SORT_COLUMNS'='s1')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $datamap11 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='id', 'BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $datamap12 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='name', 'BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $datamap13 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city', 'BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)

    // create a table and 1 bloom datamap on it, this datamap contains 3 index columns
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128', 'SORT_COLUMNS'='s1')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $datamap2 ON TABLE $normalTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='id, name, city', 'BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)

    (0 until iterations).foreach { p =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${bigFile}' INTO TABLE $bloomDMSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${bigFile}' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    var res = sql(s"explain select * from $bloomDMSampleTable where id = 1 and city = 'city_1' and name='n1'")
    checkExistence(res, true, datamap11, datamap12, datamap13)
    res = sql(s"explain select * from $normalTable where id = 1 and city = 'city_1' and name='n1'")
    checkExistence(res, true, datamap2)
    // in the following cases, default blocklet datamap will prune all the blocklets
    // and bloomfilter datamap will not take effects
    res = sql(s"explain select * from $bloomDMSampleTable where id < 0")
    checkExistence(res, false, datamap11, datamap12, datamap13)
    res = sql(s"explain select * from $normalTable where id < 0")
    checkExistence(res, false, datamap2)

    // we do not care about the datamap name here, only to validate the query results are them same
    checkQuery("fakeDm", shouldHit = false)
  }

  test("test create datamaps on different column but hit only one") {
    val originDistributedDatamapStatus = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
      CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT
    )

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP, "true")
    val datamap1 = "datamap1"
    val datamap2 = "datamap2"
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT)
         | STORED BY 'carbondata'
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $datamap1 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='name', 'BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $datamap2 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city', 'BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)

    sql(
      s"""
         | INSERT INTO $bloomDMSampleTable
         | VALUES(5,'a','beijing',21),(6,'b','shanghai',25),(7,'b','guangzhou',28)
      """.stripMargin)
    assert(sql(s"SELECT * FROM $bloomDMSampleTable WHERE city='shanghai'").count() == 1)

    // recover original setting
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
        originDistributedDatamapStatus)
  }

  test("test block change datatype for bloomfilter index datamap") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         | """.stripMargin)

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $normalTable
         | USING 'bloomfilter' WITH DEFERRED REBUILD
         | DMProperties( 'INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)
    val exception: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"ALTER TABLE $normalTable CHANGE id id bigint")
    }
    assert(exception.getMessage.contains(
      "alter table change datatype is not supported for index datamap"))
  }

  test("test drop index columns for bloomfilter datamap") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $normalTable
         | USING 'bloomfilter'
         | WITH DEFERRED REBUILD
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)
    val exception: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"alter table $normalTable drop columns(name, id)")
    }
    assert(exception.getMessage.contains(
      "alter table drop column is not supported for index datamap"))
  }

  test("test bloom datamap: bloom index column is local dictionary") {
    sql(
      s"""
         | CREATE TABLE $normalTable(c1 string, c2 int, c3 string)
         | STORED BY 'carbondata'
         | """.stripMargin)
    // c1 is local dictionary and will create bloom index on it
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(c1 string, c2 int, c3 string)
         | STORED BY 'carbondata'
         | TBLPROPERTIES('local_dictionary_include'='c1', 'local_dictionary_threshold'='1000')
         | """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP $dataMapName on table $bloomDMSampleTable
         | using 'bloomfilter'
         | DMPROPERTIES('index_columns'='c1, c2')
         | """.stripMargin)
    sql(
      s"""
         | INSERT INTO $bloomDMSampleTable
         | values ('c1v11', 11, 'c3v11'), ('c1v12', 12, 'c3v12')
         | """.stripMargin)
    sql(
      s"""
         | INSERT INTO $normalTable values ('c1v11', 11, 'c3v11'),
         | ('c1v12', 12, 'c3v12')
         | """.stripMargin)
    checkAnswer(sql(s"select * from $bloomDMSampleTable"),
      sql(s"select * from $normalTable"))
    checkAnswer(sql(s"select * from $bloomDMSampleTable where c1 = 'c1v12'"),
      sql(s"select * from $normalTable where c1 = 'c1v12'"))
  }

  test("test create bloomfilter datamap which index column datatype is complex ") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city Array<INT>, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata'
         | """.stripMargin)
    val exception: MalformedDataMapCommandException = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP $dataMapName ON TABLE $normalTable
           | USING 'bloomfilter'
           | WITH DEFERRED REBUILD
           | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
           | """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "BloomFilter datamap does not support complex datatype column"))
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
