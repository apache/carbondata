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

import org.apache.spark.sql.{CarbonUtils, DataFrame, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.util.CarbonProperties

class BloomCoarseGrainDataMapSuite extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val carbonSession = sqlContext.sparkSession
  val bigFile = s"$resourcesPath/bloom_datamap_input_big.csv"
  val smallFile = s"$resourcesPath/bloom_datamap_input_small.csv"
  val normalTable = "carbon_normal"
  val bloomDMSampleTable = "carbon_bloom"
  val dataMapName = "bloom_dm"

  override protected def beforeAll(): Unit = {
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
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

  // for CARBONDATA-2820, we will first block deferred rebuild for bloom
  test("test block deferred rebuild for bloom") {
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    val deferredRebuildException = intercept[MalformedDataMapCommandException] {
      sql(
        s"""
           | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
           | USING 'bloomfilter'
           | WITH DEFERRED REBUILD
           | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)
    }
    assert(deferredRebuildException.getMessage.contains(
      s"DEFERRED REBUILD is not supported on this datamap $dataMapName with provider bloomfilter"))

    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)
    val exception = intercept[MalformedDataMapCommandException] {
      sql(s"REBUILD DATAMAP $dataMapName ON TABLE $bloomDMSampleTable")
    }
    assert(exception.getMessage.contains(s"Non-lazy datamap $dataMapName does not support rebuild"))
  }

  ignore("test create bloom datamap and REBUILD DATAMAP") {
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

  ignore("test create bloom datamap with DEFERRED REBUILD, query hit datamap") {
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

  ignore("test create bloom datamap with DEFERRED REBUILD, query not hit datamap") {
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
         | USING 'bloomfilter'
         | DMProperties( 'INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)
    val changeDataTypeException: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"ALTER TABLE $normalTable CHANGE id id bigint")
    }
    assert(changeDataTypeException.getMessage.contains(
      "alter table change datatype is not supported for index datamap"))
    val columnRenameException: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"ALTER TABLE $normalTable CHANGE id test int")
    }
    assert(columnRenameException.getMessage.contains(
      "alter table column rename is not supported for index datamap"))
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
           | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
           | """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "BloomFilter datamap does not support complex datatype column"))
  }

  test("test create bloom datamap on newly added column") {
    val datamap1 = "datamap1"
    val datamap2 = "datamap2"
    val datamap3 = "datamap3"

    // create a table with dict/noDict/measure column
    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED BY 'carbondata' TBLPROPERTIES('table_blocksize'='128',
         | 'DICTIONARY_INCLUDE'='s1,s2', 'CACHE_LEVEL'='BLOCKLET')
         |  """.stripMargin)

    // load data into table (segment0)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomDMSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    // create simple datamap on segment0
    sql(
      s"""
         | CREATE DATAMAP $datamap1 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='id', 'BLOOM_SIZE'='640000')
      """.stripMargin)

    // add some columns including dict/noDict/measure column
    sql(
      s"""
         | ALTER TABLE $bloomDMSampleTable
         | ADD COLUMNS(num1 INT, dictString STRING, noDictString STRING)
         | TBLPROPERTIES('DEFAULT.VALUE.num1'='999', 'DEFAULT.VALUE.dictString'='old',
         | 'DICTIONARY_INCLUDE'='dictString'
         | )
         """.stripMargin)

    // load data into table (segment1)
    sql(
      s"""
         | INSERT INTO TABLE $bloomDMSampleTable VALUES
         | (100,'name0','city0',10,'s10','s20','s30','s40','s50','s60','s70','s80',0,'S01','S02'),
         | (101,'name1','city1',11,'s11','s21','s31','s41','s51','s61','s71','s81',4,'S11','S12'),
         | (102,'name2','city2',12,'s12','s22','s32','s42','s52','s62','s72','s82',5,'S21','S22')
           """.stripMargin)

    // check data after columns added
    var res = sql(
      s"""
         | SELECT name, city, num1, dictString, noDictString
         | FROM $bloomDMSampleTable
         | WHERE id = 101
         | """.stripMargin)
    checkExistence(res, true, "999", "null")

    // create datamap on newly added column
    sql(
      s"""
         | CREATE DATAMAP $datamap2 ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='s1,dictString,s8,noDictString,age,num1',
         | 'BLOOM_SIZE'='640000')
      """.stripMargin)

    // load data into table (segment2)
    sql(
      s"""
         | INSERT INTO TABLE $bloomDMSampleTable VALUES
         | (100,'name0','city0',10,'s10','s20','s30','s40','s50','s60','s70','s80',1,'S01','S02'),
         | (101,'name1','city1',11,'s11','s21','s31','s41','s51','s61','s71','s81',2,'S11','S12'),
         | (102,'name2','city1',12,'s12','s22','s32','s42','s52','s62','s72','s82',3,'S21','S22')
           """.stripMargin)

    var explainString = sql(
      s"""
         | explain SELECT id, name, num1, dictString
         | FROM $bloomDMSampleTable
         | WHERE num1 = 1
           """.stripMargin).collect()

    assert(explainString(0).getString(0).contains(
      """
        |Table Scan on carbon_bloom
        | - total: 3 blocks, 3 blocklets
        | - filter: (num1 <> null and num1 = 1)
        | - pruned by Main DataMap
        |    - skipped: 1 blocks, 1 blocklets
        | - pruned by CG DataMap
        |    - name: datamap2
        |    - provider: bloomfilter
        |    - skipped: 1 blocks, 1 blocklets""".stripMargin))

    explainString = sql(
      s"""
         | explain SELECT id, name, num1, dictString
         | FROM $bloomDMSampleTable
         | WHERE dictString = 'S21'
           """.stripMargin).collect()

    assert(explainString(0).getString(0).contains(
      """
        |Table Scan on carbon_bloom
        | - total: 3 blocks, 3 blocklets
        | - filter: (dictstring <> null and dictstring = S21)
        | - pruned by Main DataMap
        |    - skipped: 1 blocks, 1 blocklets
        | - pruned by CG DataMap
        |    - name: datamap2
        |    - provider: bloomfilter
        |    - skipped: 0 blocks, 0 blocklets""".stripMargin))

  }

  test("test bloom datamap on all basic data types") {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    val columnNames = "booleanField,shortField,intField,bigintField,doubleField,stringField," +
      "timestampField,decimalField,dateField,charField,floatField"

    sql(
      s"""
         | CREATE TABLE $bloomDMSampleTable(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE $normalTable(
         |    booleanField boolean,
         |    shortField smallint,
         |    intField int,
         |    bigintField bigint,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField string,
         |    floatField float
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    // first data load
    sql(
      s"""
         | INSERT INTO TABLE $bloomDMSampleTable
         | VALUES(true,1,10,100,48.4,'spark','2015-4-23 12:01:01',1.23,'2015-4-23','aaa',2.5),
         | (true,1,11,100,44.4,'flink','2015-5-23 12:01:03',23.23,'2015-5-23','ccc',2.15),
         | (true,3,14,160,43.4,'hive','2015-7-26 12:01:06',3454.32,'2015-7-26','ff',5.5),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $normalTable
         | VALUES(true,1,10,100,48.4,'spark','2015-4-23 12:01:01',1.23,'2015-4-23','aaa',2.5),
         | (true,1,11,100,44.4,'flink','2015-5-23 12:01:03',23.23,'2015-5-23','ccc',2.15),
         | (true,3,14,160,43.4,'hive','2015-7-26 12:01:06',3454.32,'2015-7-26','ff',5.5),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)

    // create datamap
    sql(
      s"""
         | CREATE DATAMAP dm_test ON TABLE $bloomDMSampleTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='$columnNames',
         | 'BLOOM_SIZE'='640000')
      """.stripMargin)

    // second data load
    sql(
      s"""
         | INSERT INTO TABLE $bloomDMSampleTable
         | VALUES(true,1,10,100,48.4,'spark','2015-4-23 12:01:01',1.23,'2015-4-23','aaa',2.5),
         | (true,1,11,100,44.4,'flink','2015-5-23 12:01:03',23.23,'2015-5-23','ccc',2.15),
         | (true,3,14,160,43.4,'hive','2015-7-26 12:01:06',3454.32,'2015-7-26','ff',5.5),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)
    sql(
      s"""
         | INSERT INTO TABLE $normalTable
         | VALUES(true,1,10,100,48.4,'spark','2015-4-23 12:01:01',1.23,'2015-4-23','aaa',2.5),
         | (true,1,11,100,44.4,'flink','2015-5-23 12:01:03',23.23,'2015-5-23','ccc',2.15),
         | (true,3,14,160,43.4,'hive','2015-7-26 12:01:06',3454.32,'2015-7-26','ff',5.5),
         | (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
       """.stripMargin)

    // check simply query
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE booleanField = true"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField = true"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE shortField = 3"),
      sql(s"SELECT * FROM $normalTable WHERE shortField = 3"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE intField = 14"),
      sql(s"SELECT * FROM $normalTable WHERE intField = 14"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE bigintField = 100"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField = 100"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doubleField = 43.4"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField = 43.4"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE stringField = 'spark'"),
      sql(s"SELECT * FROM $normalTable WHERE stringField = 'spark'"))
    checkAnswer(
      sql(s"SELECT * FROM $bloomDMSampleTable WHERE timestampField = '2015-7-26 12:01:06'"),
      sql(s"SELECT * FROM $normalTable WHERE timestampField = '2015-7-26 12:01:06'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE decimalField = 23.23"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField = 23.23"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE dateField = '2015-4-23'"),
      sql(s"SELECT * FROM $normalTable WHERE dateField = '2015-4-23'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE charField = 'ccc'"),
      sql(s"SELECT * FROM $normalTable WHERE charField = 'ccc'"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE floatField = 2.5"),
      sql(s"SELECT * FROM $normalTable WHERE floatField = 2.5"))

    // check query using null
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE booleanField is null"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE shortField is null"),
      sql(s"SELECT * FROM $normalTable WHERE shortField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE intField is null"),
      sql(s"SELECT * FROM $normalTable WHERE intField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE bigintField is null"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doubleField is null"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE stringField is null"),
      sql(s"SELECT * FROM $normalTable WHERE stringField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE timestampField is null"),
      sql(s"SELECT * FROM $normalTable WHERE timestampField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE decimalField is null"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE dateField is null"),
      sql(s"SELECT * FROM $normalTable WHERE dateField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE charField is null"),
      sql(s"SELECT * FROM $normalTable WHERE charField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE floatField is null"),
      sql(s"SELECT * FROM $normalTable WHERE floatField is null"))

    // check default `NullValue` of measure does not affect result
    // Note: Test data has row contains NULL for each column but no corresponding `NullValue`,
    // so we should get 0 row if query uses the `NullValue`
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE booleanField = false"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField = false"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE shortField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE shortField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE intField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE intField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE bigintField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE doubleField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE decimalField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomDMSampleTable WHERE floatField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE floatField = 0"))

  }

  test("test bloom datamap on multiple columns") {
    sql("drop table if exists store")
        sql(
          s"""
             |CREATE TABLE IF NOT EXISTS store(
             | market_code STRING,
             | device_code STRING,
             | country_code STRING,
             | category_id INTEGER,
             | product_id string,
             | date date,
             | est_free_app_download LONG,
             | est_paid_app_download LONG,
             | est_revenue LONG
             | )
             | STORED BY 'carbondata'
             | TBLPROPERTIES(
             | 'SORT_COLUMNS'='market_code, device_code, country_code, category_id, date,product_id',
             | 'NO_INVERTED_INDEX'='est_free_app_download, est_paid_app_download,est_revenue',
             | 'DICTIONARY_INCLUDE' = 'market_code, device_code, country_code,category_id, product_id',
             | 'SORT_SCOPE'='GLOBAL_SORT',
             | 'CACHE_LEVEL'='BLOCKLET',  'TABLE_BLOCKSIZE'='256',
             | 'GLOBAL_SORT_PARTITIONS'='2'
             | )""".stripMargin)

    sql(s"""insert into store values('a', 'ios-phone', 'EE', 100021, 590416158, '2016-09-01', 100, 200, 300)""")
    sql(s"""insert into store values('b', 'ios-phone', 'EE', 100021, 590437560, '2016-09-03', 100, 200, 300)""")
    sql(s"""insert into store values('a', 'ios-phone', 'EF', 100022, 590416159, '2016-09-04', 100, 200, 300)""")

    sql(
      s"""
         |CREATE DATAMAP IF NOT EXISTS bloomfilter_all_dimensions ON TABLE store
         | USING 'bloomfilter'
         | DMPROPERTIES (
         | 'INDEX_COLUMNS'='market_code, device_code, country_code, category_id, date,product_id',
         | 'BLOOM_SIZE'='640000',
         | 'BLOOM_FPP'='0.000001',
         | 'BLOOM_COMPRESS'='true'
         | )
       """.stripMargin).show()

    checkAnswer(sql(
      s"""SELECT market_code, device_code, country_code,
         |category_id, sum(est_free_app_download) FROM store WHERE date
         |BETWEEN '2016-09-01' AND '2016-09-03' AND device_code='ios-phone'
         |AND country_code='EE' AND category_id=100021 AND product_id IN (590416158, 590437560)
         |GROUP BY date, market_code, device_code, country_code, category_id""".stripMargin),
      Seq(Row("a", "ios-phone", "EE", 100021, 100), Row("b", "ios-phone", "EE", 100021, 100)))

    assert(sql(
      s"""SELECT market_code, device_code, country_code,
         |category_id, sum(est_free_app_download) FROM store WHERE (device_code='ios-phone'
         |AND country_code='EF') or (category_id=100021 AND product_id IN (590416158, 590437560))
         |GROUP BY date, market_code, device_code, country_code, category_id""".stripMargin).collect().length == 3)

    checkAnswer(sql("select device_code from store where product_id=590416158"), Seq(Row("ios-phone")))

    sql("drop table if exists store")
  }

  override protected def afterAll(): Unit = {
    deleteFile(bigFile)
    deleteFile(smallFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomDMSampleTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
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
