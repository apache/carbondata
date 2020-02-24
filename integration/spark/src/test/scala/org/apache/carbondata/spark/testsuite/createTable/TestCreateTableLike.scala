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

import java.util

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class TestCreateTableLike extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll{

  override protected def beforeAll(): Unit = {
    sql("drop table if exists pt_tbl")
    sql("drop table if exists hive_pt")
    sql("drop table if exists bkt_tbl")
    sql("drop table if exists stream_tbl")
    sql("drop table if exists sourceTable")
    sql(
      """create table sourceTable
        |(a int, b string)
        |STORED AS carbondata
        |TBLPROPERTIES(
        |  'SORT_COLUMNS'='b',
        |  'SORT_SCOPE'='GLOBAL_SORT',
        |  'LOCAL_DICTIONARY_ENABLE'='false',
        |  'carbon.column.compress'='zstd',
        |  'CACHE_LEVEL'='blocklet',
        |  'COLUMN_META_CACHE'='a',
        |  'TABLE_BLOCKSIZE'='256',
        |  'TABLE_BLOCKLET_SIZE'='16')
        |  """.stripMargin)
    sql("insert into sourceTable values(5,'bb'),(6,'cc')")
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists pt_tbl")
    sql("drop table if exists hive_pt")
    sql("drop table if exists bkt_tbl")
    sql("drop table if exists stream_tbl")
    sql("drop table if exists sourceTable")
  }

  override protected def beforeEach(): Unit = {
    sql("drop table if exists targetTable")
  }

  override protected def afterEach(): Unit = {
    sql("drop table if exists targetTable")
  }

  def checkColumns(left: util.List[ColumnSchema], right: util.List[ColumnSchema]): Boolean = {
    if (left.size != right.size) {
      false
    } else {
      for(i <- 0 until left.size()) {
        if (!left.get(i).equals(right.get(i))) {
          return false
        }
      }
      true
    }
  }

  def checkTableProperties(src: TableIdentifier, dst: TableIdentifier): Unit = {
    val info_src = CarbonEnv.getCarbonTable(src)(sqlContext.sparkSession).getTableInfo
    val info_dst = CarbonEnv.getCarbonTable(dst)(sqlContext.sparkSession).getTableInfo

    val fact_src = info_src.getFactTable
    val fact_dst = info_dst.getFactTable

    // check column schemas same
    assert(checkColumns(fact_src.getListOfColumns, fact_dst.getListOfColumns))

    // check table properties same
    assert(fact_src.getTableProperties.equals(fact_dst.getTableProperties))

    // check transaction same
    assert(!(info_src.isTransactionalTable ^ info_dst.isTransactionalTable))

    // check bucket info same
    if (null == fact_src.getBucketingInfo) {
      assert(null == fact_dst.getBucketingInfo)
    } else {
      assert(null != fact_dst.getBucketingInfo)
      assert(fact_src.getBucketingInfo.getNumOfRanges == fact_dst.getBucketingInfo.getNumOfRanges)
      assert(checkColumns(fact_src.getBucketingInfo.getListOfColumns,fact_dst.getBucketingInfo.getListOfColumns))
    }

    // check partition info same
    if (null == fact_src.getPartitionInfo) {
      assert(null == fact_dst.getPartitionInfo)
    } else {
      assert(null != fact_dst.getPartitionInfo)
      assert(fact_src.getPartitionInfo.getPartitionType == fact_dst.getPartitionInfo.getPartitionType)
      assert(checkColumns(fact_src.getPartitionInfo.getColumnSchemaList, fact_dst.getPartitionInfo.getColumnSchemaList))
    }

    // check different id
    assert(!info_src.getTableUniqueName.equals(info_dst.getTableUniqueName))
    assert(!info_src.getOrCreateAbsoluteTableIdentifier().getTablePath.equals(info_dst.getOrCreateAbsoluteTableIdentifier.getTablePath))
    assert(!info_src.getFactTable.getTableId.equals(info_dst.getFactTable.getTableId))
    assert(!info_src.getFactTable.getTableName.equals(info_dst.getFactTable.getTableName))
  }

  test("create table like simple table") {
    sql("create table targetTable like sourceTable")
    checkTableProperties(TableIdentifier("sourceTable"), TableIdentifier("targetTable"))
  }

  test("test same table name") {
    val exception = intercept[TableAlreadyExistsException] {
      sql("create table sourceTable like sourceTable")
    }
    assert(exception.getMessage.contains("already exists in database"))
  }

  // ignore this test case since Spark 2.1 does not support specify location
  // and also current implementation in carbon does not use this parameter. 
  ignore("command with location") {
    sql(s"create table targetTable like sourceTable location '$warehouse/tbl_with_loc' ")
    checkTableProperties(TableIdentifier("sourceTable"), TableIdentifier("targetTable"))
  }

  test("table with datamap") {
    // datamap relation does not store in parent table
    sql(
      s"""
         | CREATE DATAMAP dm1 ON TABLE sourceTable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='b', 'BLOOM_SIZE'='32000')
      """.stripMargin)
    sql("create table targetTable like sourceTable")
    checkTableProperties(TableIdentifier("sourceTable"), TableIdentifier("targetTable"))
  }

  test("table with hive partition") {
    sql(
      """
        | CREATE TABLE hive_pt (
        | a int, b string)
        | PARTITIONED BY (id int)
        | STORED AS carbondata
      """.stripMargin)
    sql("create table targetTable like hive_pt")
    checkTableProperties(TableIdentifier("hive_pt"), TableIdentifier("targetTable"))
  }

  test("table with bucket") {
    sql("""
        | CREATE TABLE IF NOT EXISTS bkt_tbl (
        |   a int, b string
        | ) STORED AS carbondata
        | TBLPROPERTIES ('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='b')
        | """.stripMargin)

    sql("create table targetTable like bkt_tbl")
    checkTableProperties(TableIdentifier("bkt_tbl"), TableIdentifier("targetTable"))
  }

  test("table with streaming") {
    sql("""
          | CREATE TABLE IF NOT EXISTS stream_tbl (
          |   a int, b string
          | ) STORED AS carbondata
          | TBLPROPERTIES ('streaming' = 'true')
          | """.stripMargin)

    sql("create table targetTable like stream_tbl")
    checkTableProperties(TableIdentifier("stream_tbl"), TableIdentifier("targetTable"))
  }

  test("table with schema changed") {
    sql("ALTER TABLE sourceTable ADD COLUMNS(charField STRING) " +
      "TBLPROPERTIES ('DEFAULT.VALUE.charfield'='def')")
    sql("create table targetTable like sourceTable")
    checkTableProperties(TableIdentifier("sourceTable"), TableIdentifier("targetTable"))
  }

}
