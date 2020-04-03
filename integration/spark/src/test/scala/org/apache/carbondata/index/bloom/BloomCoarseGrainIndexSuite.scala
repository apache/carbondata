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

package org.apache.carbondata.index.bloom

import java.io.{File, PrintWriter}
import java.util.UUID

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedIndexCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.status.{DataMapStatusManager, IndexStatus}
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider
import org.apache.carbondata.core.util.CarbonProperties

class BloomCoarseGrainIndexSuite extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val carbonSession = sqlContext.sparkSession
  val bigFile = s"$resourcesPath/bloom_datamap_input_big.csv"
  val smallFile = s"$resourcesPath/bloom_datamap_input_small.csv"
  val normalTable = "carbon_normal"
  val bloomSampleTable = "carbon_bloom"
  val indexName = "bloom_dm"

  override protected def beforeAll(): Unit = {
    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    createFile(bigFile, line = 50000)
    createFile(smallFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  private def checkSqlHitDataMap(sqlText: String, dataMapName: String, shouldHit: Boolean): DataFrame = {
    // ignore checking index hit, because bloom bloom index may be skipped if
    // default blocklet index pruned all the blocklets.
    // We cannot tell whether the index will be hit from the query.
    sql(sqlText)
  }

  private def checkQuery(dataMapName: String, shouldHit: Boolean = true) = {
    /**
     * queries that use equal operator
     */
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id = 1", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 1"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id = 999", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 999"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city = 'city_1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_1'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city = 'city_999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999'"))
    // query with two index_columns
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id = 1 and city='city_1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 1 and city='city_1'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id = 999 and city='city_999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id = 999 and city='city_999'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city = 'city_1' and id = 0", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_1' and id = 0"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city = 'city_999' and name='n999'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999' and name='n999'"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city = 'city_999' and name='n1'", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city = 'city_999' and name='n1'"))

    /**
     * queries that use in operator
     */
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id in (1)", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (1)"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id in (999)", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (999)"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city in( 'city_1')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in( 'city_1')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city in ('city_999')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_999')"))
    // query with two index_columns
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id in (1) and city in ('city_1')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (1) and city in ('city_1')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where id in (999) and city in ('city_999')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where id in (999) and city in ('city_999')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city in ('city_1') and id in (0)", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_1') and id in (0)"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city in ('city_999') and name in ('n999')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_999') and name in ('n999')"))
    checkAnswer(
      checkSqlHitDataMap(s"select * from $bloomSampleTable where city in ('city_999') and name in ('n1')", dataMapName, shouldHit),
      sql(s"select * from $normalTable where city in ('city_999') and name in ('n1')"))

    checkAnswer(
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $bloomSampleTable"),
      sql(s"select min(id), max(id), min(name), max(name), min(city), max(city)" +
          s" from $normalTable"))
  }

  test("test create bloom index on table with existing data") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(), sqlContext.sparkSession, CarbonIndexProvider.BLOOMFILTER)

    // load two segments
    (1 to 2).foreach { i =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(), sqlContext.sparkSession, CarbonIndexProvider.BLOOMFILTER)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").show(false)
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    checkQuery(indexName)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  // for CARBONDATA-2820, we will first block deferred refresh for bloom
  test("test block deferred refresh for bloom") {
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    val deferredRebuildException = intercept[MalformedIndexCommandException] {
      sql(
        s"""
           | CREATE INDEX $indexName
           | ON $bloomSampleTable (city, id)
           | AS 'bloomfilter'
           | WITH DEFERRED REFRESH
           | properties('BLOOM_SIZE'='640000')
      """.stripMargin)
    }
    assert(deferredRebuildException.getMessage.contains(
      s"DEFERRED REFRESH is not supported on this index $indexName with provider bloomfilter"))

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)
    val exception = intercept[MalformedIndexCommandException] {
      sql(s"REFRESH INDEX $indexName ON TABLE $bloomSampleTable")
    }
    assert(exception.getMessage.contains(s"Non-lazy index $indexName does not support manual refresh"))
  }

  ignore("test create bloom index and REFRESH INDEX") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
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
           | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").show(false)
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    checkQuery(indexName)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  ignore("test create bloom index WITH DEFERRED REFRESH, query hit index") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
       """.stripMargin)

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | WITH DEFERRED REFRESH
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.DISABLED.name(), sqlContext.sparkSession, CarbonIndexProvider.BLOOMFILTER)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.DISABLED.name(), sqlContext.sparkSession, CarbonIndexProvider.BLOOMFILTER)

    // once we rebuild, it should be enabled
    sql(s"REFRESH INDEX $indexName ON $bloomSampleTable")
    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(), sqlContext.sparkSession, CarbonIndexProvider.BLOOMFILTER)

    sql(s"SHOW INDEXES ON $bloomSampleTable").show(false)
    checkExistence(sql(s"SHOW INDEXES ON $bloomSampleTable"), true, indexName)
    checkQuery(indexName)

    // once we load again, index should be disabled, since it is lazy
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)
    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.DISABLED.name(), sqlContext.sparkSession, CarbonIndexProvider.BLOOMFILTER)

    checkQuery(indexName, shouldHit = false)

    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  ignore("test create bloom index WITH DEFERRED REFRESH, query not hit index") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
       """.stripMargin)

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | WITH DEFERRED REFRESH
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)

    // index is not loaded, so it should not hit
    checkQuery(indexName, shouldHit = false)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test bloom index: multiple indexes with each on one column vs one index on multiple columns") {
    val iterations = 1
    // 500000 lines will result to 3 blocklets and bloomfilter index will prune 2 blocklets.
    val datamap11 = "datamap11"
    val datamap12 = "datamap12"
    val datamap13 = "datamap13"
    val datamap2 = "datamap2"

    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
    // create a table and 3 bloom indexes on it, each index contains one index column
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'SORT_COLUMNS'='s1')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $datamap11
         | ON $bloomSampleTable (id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $datamap12
         | ON $bloomSampleTable (name)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $datamap13
         | ON $bloomSampleTable (city)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)

    // create a table and 1 bloom index on it, this index contains 3 index columns
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'SORT_COLUMNS'='s1')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $datamap2
         | ON $normalTable (id, name, city)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)

    (0 until iterations).foreach { p =>
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${bigFile}' INTO TABLE $bloomSampleTable
           | OPTIONS('header'='false')
         """.stripMargin)
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '${bigFile}' INTO TABLE $normalTable
           | OPTIONS('header'='false')
         """.stripMargin)
    }

    var res = sql(s"explain select * from $bloomSampleTable where id = 1 and city = 'city_1' and name='n1'")
    checkExistence(res, true, datamap11, datamap12, datamap13)
    res = sql(s"explain select * from $normalTable where id = 1 and city = 'city_1' and name='n1'")
    checkExistence(res, true, datamap2)
    // in the following cases, default blocklet index will prune all the blocklets
    // and bloomfilter index will not take effects
    res = sql(s"explain select * from $bloomSampleTable where id < 0")
    checkExistence(res, false, datamap11, datamap12, datamap13)
    res = sql(s"explain select * from $normalTable where id < 0")
    checkExistence(res, false, datamap2)

    // we do not care about the index name here, only to validate the query results are them same
    checkQuery("fakeDm", shouldHit = false)
  }

  test("test create indexs on different column but hit only one") {
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
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT)
         | STORED AS carbondata
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $datamap1
         | ON $bloomSampleTable (name)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $datamap2
         | ON $bloomSampleTable (city)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='64000', 'BLOOM_FPP'='0.00001')
      """.stripMargin)

    sql(
      s"""
         | INSERT INTO $bloomSampleTable
         | VALUES(5,'a','beijing',21),(6,'b','shanghai',25),(7,'b','guangzhou',28)
      """.stripMargin)
    assert(sql(s"SELECT * FROM $bloomSampleTable WHERE city='shanghai'").count() == 1)

    // recover original setting
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
      originDistributedDatamapStatus)
  }

  test("test block change datatype for bloomfilter index") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         | """.stripMargin)

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $normalTable (city, id)
         | AS 'bloomfilter'
         | properties( 'BLOOM_SIZE'='640000')
      """.stripMargin)
    val changeDataTypeException: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"ALTER TABLE $normalTable CHANGE id id bigint")
    }
    assert(changeDataTypeException.getMessage.contains(
      "alter table change datatype is not supported for index"))
    val columnRenameException: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"ALTER TABLE $normalTable CHANGE id test int")
    }
    assert(columnRenameException.getMessage.contains(
      "alter table column rename is not supported for index"))
  }

  test("test drop index columns for bloomfilter index") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $normalTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)
    val exception: MalformedCarbonCommandException = intercept[MalformedCarbonCommandException] {
      sql(s"alter table $normalTable drop columns(name, id)")
    }
    assert(exception.getMessage.contains(
      "alter table drop column is not supported for index"))
  }

  test("test bloom index: bloom index column is local dictionary") {
    sql(
      s"""
         | CREATE TABLE $normalTable(c1 string, c2 int, c3 string)
         | STORED AS carbondata
         | """.stripMargin)
    // c1 is local dictionary and will create bloom index on it
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(c1 string, c2 int, c3 string)
         | STORED AS carbondata
         | TBLPROPERTIES('local_dictionary_include'='c1', 'local_dictionary_threshold'='1000')
         | """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | on $bloomSampleTable (c1,c2)
         | as 'bloomfilter'
         | """.stripMargin)
    sql(
      s"""
         | INSERT INTO $bloomSampleTable
         | values ('c1v11', 11, 'c3v11'), ('c1v12', 12, 'c3v12')
         | """.stripMargin)
    sql(
      s"""
         | INSERT INTO $normalTable values ('c1v11', 11, 'c3v11'),
         | ('c1v12', 12, 'c3v12')
         | """.stripMargin)
    checkAnswer(sql(s"select * from $bloomSampleTable"),
      sql(s"select * from $normalTable"))
    checkAnswer(sql(s"select * from $bloomSampleTable where c1 = 'c1v12'"),
      sql(s"select * from $normalTable where c1 = 'c1v12'"))
  }

  test("test create bloomfilter index which index column datatype is complex ") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city Array<INT>, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | """.stripMargin)
    val exception: MalformedIndexCommandException = intercept[MalformedIndexCommandException] {
      sql(
        s"""
           | CREATE INDEX $indexName
           | ON $normalTable (city, id)
           | AS 'bloomfilter'
           | properties('BLOOM_SIZE'='640000')
           | """.stripMargin)
    }
    assert(exception.getMessage.contains(
      "BloomFilter does not support complex datatype column"))
  }

  test("test create bloomfilter index which index column datatype is Binary ") {
    sql("drop table if exists binaryTable")
    sql(
      "CREATE TABLE binaryTable (CUST_ID binary,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,36),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata")
    val exception: MalformedIndexCommandException = intercept[MalformedIndexCommandException] {
      sql(
        s"""
           | CREATE INDEX binaryBloom
           | ON binaryTable (cust_id)
           | AS 'bloomfilter'
           | properties('BLOOM_SIZE'='640000')
           | """.stripMargin)
    }
    assert(exception.getMessage.equalsIgnoreCase(
      "BloomFilter does not support binary datatype column: cust_id"  ))
  }

  test("test create bloom index on newly added column") {
    // Fix the loading cores to ensure number of buckets.
    CarbonProperties.getInstance().addProperty("carbon.number.of.cores.while.loading","1")

    val datamap1 = "datamap1"
    val datamap2 = "datamap2"
    val datamap3 = "datamap3"

    // create a table with dict/noDict/measure column
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128','CACHE_LEVEL'='BLOCKLET')
         |  """.stripMargin)

    // load data into table (segment0)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$smallFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    // create simple index on segment0
    sql(
      s"""
         | CREATE INDEX $datamap1
         | ON $bloomSampleTable (id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    // add some columns including dict/noDict/measure column
    sql(
      s"""
         | ALTER TABLE $bloomSampleTable
         | ADD COLUMNS(num1 INT, dictString STRING, noDictString STRING)
         | TBLPROPERTIES('DEFAULT.VALUE.num1'='999', 'DEFAULT.VALUE.dictString'='old')
         """.stripMargin)

    // load data into table (segment1)
    sql(
      s"""
         | INSERT INTO TABLE $bloomSampleTable VALUES
         | (100,'name0','city0',10,'s10','s20','s30','s40','s50','s60','s70','s80',0,'S01','S02'),
         | (101,'name1','city1',11,'s11','s21','s31','s41','s51','s61','s71','s81',4,'S11','S12'),
         | (102,'name2','city2',12,'s12','s22','s32','s42','s52','s62','s72','s82',5,'S21','S22')
           """.stripMargin)

    // check data after columns added
    var res = sql(
      s"""
         | SELECT name, city, num1, dictString, noDictString
         | FROM $bloomSampleTable
         | WHERE id = 101
         | """.stripMargin)
    checkExistence(res, true, "999", "null")

    // create index on newly added column
    sql(
      s"""
         | CREATE INDEX $datamap2
         | ON $bloomSampleTable (s1,dictString,s8,noDictString,age,num1)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    // load data into table (segment2)
    sql(
      s"""
         | INSERT INTO TABLE $bloomSampleTable VALUES
         | (100,'name0','city0',10,'s10','s20','s30','s40','s50','s60','s70','s80',1,'S01','S02'),
         | (101,'name1','city1',11,'s11','s21','s31','s41','s51','s61','s71','s81',2,'S11','S12'),
         | (102,'name2','city1',12,'s12','s22','s32','s42','s52','s62','s72','s82',3,'S21','S22')
           """.stripMargin)

    var explainString = sql(
      s"""
         | explain SELECT id, name, num1, dictString
         | FROM $bloomSampleTable
         | WHERE num1 = 1
           """.stripMargin).collect()

    assert(explainString(0).getString(0).contains(
      """
        |Table Scan on carbon_bloom
        | - total: 3 blocks, 3 blocklets
        | - filter: (num1 <> null and num1 = 1)
        | - pruned by Main Index
        |    - skipped: 1 blocks, 1 blocklets
        | - pruned by CG Index
        |    - name: datamap2
        |    - provider: bloomfilter
        |    - skipped: 1 blocks, 1 blocklets""".stripMargin))

    explainString = sql(
      s"""
         | explain SELECT id, name, num1, dictString
         | FROM $bloomSampleTable
         | WHERE dictString = 'S21'
           """.stripMargin).collect()

    assert(explainString(0).getString(0).contains(
      """
        |Table Scan on carbon_bloom
        | - total: 3 blocks, 3 blocklets
        | - filter: (dictstring <> null and dictstring = S21)
        | - pruned by Main Index
        |    - skipped: 1 blocks, 1 blocklets
        | - pruned by CG Index
        |    - name: datamap2
        |    - provider: bloomfilter
        |    - skipped: 0 blocks, 0 blocklets""".stripMargin))

  }

  test("test bloom index on all basic data types") {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    val columnNames = "booleanField,shortField,intField,bigintField,doubleField,stringField," +
                      "timestampField,decimalField,dateField,charField,floatField"

    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(
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
         | STORED AS carbondata
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
         | STORED AS carbondata
       """.stripMargin)

    // first data load
    sql(
      s"""
         | INSERT INTO TABLE $bloomSampleTable
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

    // create index
    sql(
      s"""
         | CREATE INDEX dm_test
         | ON $bloomSampleTable ($columnNames)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    // second data load
    sql(
      s"""
         | INSERT INTO TABLE $bloomSampleTable
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
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE booleanField = true"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField = true"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE shortField = 3"),
      sql(s"SELECT * FROM $normalTable WHERE shortField = 3"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE intField = 14"),
      sql(s"SELECT * FROM $normalTable WHERE intField = 14"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE bigintField = 100"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField = 100"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doubleField = 43.4"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField = 43.4"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE stringField = 'spark'"),
      sql(s"SELECT * FROM $normalTable WHERE stringField = 'spark'"))
    checkAnswer(
      sql(s"SELECT * FROM $bloomSampleTable WHERE timestampField = '2015-7-26 12:01:06'"),
      sql(s"SELECT * FROM $normalTable WHERE timestampField = '2015-7-26 12:01:06'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE decimalField = 23.23"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField = 23.23"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE dateField = '2015-4-23'"),
      sql(s"SELECT * FROM $normalTable WHERE dateField = '2015-4-23'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE charField = 'ccc'"),
      sql(s"SELECT * FROM $normalTable WHERE charField = 'ccc'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE floatField = 2.5"),
      sql(s"SELECT * FROM $normalTable WHERE floatField = 2.5"))

    // check query using null
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE booleanField is null"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE shortField is null"),
      sql(s"SELECT * FROM $normalTable WHERE shortField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE intField is null"),
      sql(s"SELECT * FROM $normalTable WHERE intField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE bigintField is null"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doubleField is null"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE stringField is null"),
      sql(s"SELECT * FROM $normalTable WHERE stringField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE timestampField is null"),
      sql(s"SELECT * FROM $normalTable WHERE timestampField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE decimalField is null"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE dateField is null"),
      sql(s"SELECT * FROM $normalTable WHERE dateField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE charField is null"),
      sql(s"SELECT * FROM $normalTable WHERE charField is null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE floatField is null"),
      sql(s"SELECT * FROM $normalTable WHERE floatField is null"))

    // check default `NullValue` of measure does not affect result
    // Note: Test data has row contains NULL for each column but no corresponding `NullValue`,
    // so we should get 0 row if query uses the `NullValue`
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE booleanField = false"),
      sql(s"SELECT * FROM $normalTable WHERE booleanField = false"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE shortField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE shortField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE intField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE intField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE bigintField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE bigintField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doubleField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE doubleField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE decimalField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE decimalField = 0"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE floatField = 0"),
      sql(s"SELECT * FROM $normalTable WHERE floatField = 0"))

  }

  test("test bloom index on multiple columns") {
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
         | STORED AS carbondata
         | TBLPROPERTIES(
         | 'SORT_COLUMNS'='market_code, device_code, country_code, category_id, date,product_id',
         | 'NO_INVERTED_INDEX'='est_free_app_download, est_paid_app_download,est_revenue',
         | 'SORT_SCOPE'='GLOBAL_SORT',
         | 'CACHE_LEVEL'='BLOCKLET',  'TABLE_BLOCKSIZE'='256',
         | 'GLOBAL_SORT_PARTITIONS'='2'
         | )""".stripMargin)

    sql(s"""insert into store values('a', 'ios-phone', 'EE', 100021, 590416158, '2016-09-01', 100, 200, 300)""")
    sql(s"""insert into store values('b', 'ios-phone', 'EE', 100021, 590437560, '2016-09-03', 100, 200, 300)""")
    sql(s"""insert into store values('a', 'ios-phone', 'EF', 100022, 590416159, '2016-09-04', 100, 200, 300)""")

    sql(
      s"""
         | CREATE INDEX IF NOT EXISTS bloomfilter_all_dimensions
         | ON store (market_code, device_code, country_code, category_id, date,product_id)
         | AS 'bloomfilter'
         | properties (
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
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
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
