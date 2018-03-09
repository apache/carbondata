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

package org.apache.carbondata.spark.testsuite.dataload

import java.io.{BufferedWriter, File, FileWriter, FilenameFilter}

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestBatchSortDataLoad extends QueryTest with BeforeAndAfterAll {
  var filePath: String = _

  def buildTestData() = {
    filePath = s"${integrationPath}/spark-common-test/target/big.csv"
    val file = new File(filePath)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write("c1,c2,c3, c4, c5, c6, c7, c8, c9, c10")
    writer.newLine()
    for(i <- 0 until 100000) {
      writer.write("a" + i%1000 + "," +
                   "b" + i%1000 + "," +
                   "c" + i%1000 + "," +
                   "d" + i%1000 + "," +
                   "e" + i%1000 + "," +
                   "f" + i%1000 + "," +
                   i%1000 + "," +
                   i%1000 + "," +
                   i%1000 + "," +
                   i%1000 + "\n")
      if ( i % 10000 == 0) {
        writer.flush()
      }
    }
    writer.close()
  }

  def dropTable() = {
    sql("DROP TABLE IF EXISTS carbon_load1")
    sql("DROP TABLE IF EXISTS carbon_load2")
    sql("DROP TABLE IF EXISTS carbon_load3")
    sql("DROP TABLE IF EXISTS carbon_load4")
    sql("DROP TABLE IF EXISTS carbon_load5")
    sql("DROP TABLE IF EXISTS carbon_load6")
  }

  override def beforeAll {
    dropTable
    buildTestData
  }
  
  test("test batch sort load by passing option to load command") {

    sql(
      """
        | CREATE TABLE carbon_load1(c1 string, c2 string, c3 string, c4 string, c5 string,
        | c6 string, c7 int, c8 int, c9 int, c10 int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('dictionary_include'='c1,c2,c3,c4,c5,c6',
        | 'sort_scope'='batch_sort')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 " +
        s"OPTIONS('batch_sort_size_inmb'='1')")

    checkAnswer(sql("select count(*) from carbon_load1"), Seq(Row(100000)))

    assert(getIndexfileCount("carbon_load1") == 5, "Something wrong in batch sort")
  }

  test("test batch sort load by passing option to load command and compare with normal load") {

    sql(
      """
        | CREATE TABLE carbon_load2(c1 string, c2 string, c3 string, c4 string, c5 string,
        | c6 string, c7 int, c8 int, c9 int, c10 int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load2 ")

    checkAnswer(sql("select * from carbon_load1 where c1='a1' order by c1"),
      sql("select * from carbon_load2 where c1='a1' order by c1"))

  }

  test("test batch sort load by passing option and compaction") {

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 " +
        s"OPTIONS('batch_sort_size_inmb'='1')")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 " +
        s"OPTIONS('batch_sort_size_inmb'='1')")
    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load1 " +
        s"OPTIONS('batch_sort_size_inmb'='1')")
    sql("alter table carbon_load1 compact 'major'")
    Thread.sleep(4000)
    checkAnswer(sql("select count(*) from carbon_load1"), Seq(Row(400000)))

    assert(getIndexfileCount("carbon_load1", "0.1") == 1, "Something wrong in compaction after batch sort")

  }

  test("test batch sort load by passing option with single pass") {

    sql(
      """
        | CREATE TABLE carbon_load3(c1 string, c2 string, c3 string, c4 string, c5 string,
        | c6 string, c7 int, c8 int, c9 int, c10 int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('dictionary_include'='c1,c2,c3,c4,c5,c6',
        | 'sort_scope'='batch_sort')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load3 " +
        s"OPTIONS('batch_sort_size_inmb'='1', 'single_pass'='true')")

    checkAnswer(sql("select count(*) from carbon_load3"), Seq(Row(100000)))

    assert(getIndexfileCount("carbon_load3") == 5, "Something wrong in batch sort")

    checkAnswer(sql("select * from carbon_load3 where c1='a1' order by c1"),
      sql("select * from carbon_load2 where c1='a1' order by c1"))

  }

  test("test batch sort load by with out passing option but through carbon properties") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "BATCH_SORT")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB, "1")
    sql(
      """
        | CREATE TABLE carbon_load4(c1 string, c2 string, c3 string, c4 string, c5 string,
        | c6 string, c7 int, c8 int, c9 int, c10 int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('dictionary_include'='c1,c2,c3,c4,c5,c6')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load4 " )

    checkAnswer(sql("select count(*) from carbon_load4"), Seq(Row(100000)))

    assert(getIndexfileCount("carbon_load4") == 5, "Something wrong in batch sort")
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
        CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB, "0")
  }

  test("test batch sort load by with out passing option but through carbon properties with default size") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "BATCH_SORT")
    sql(
      """
        | CREATE TABLE carbon_load6(c1 string, c2 string, c3 string, c4 string, c5 string,
        | c6 string, c7 int, c8 int, c9 int, c10 int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('dictionary_include'='c1,c2,c3,c4,c5,c6')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' into table carbon_load6 " )

    checkAnswer(sql("select count(*) from carbon_load6"), Seq(Row(100000)))

    assert(getIndexfileCount("carbon_load6") == 1, "Something wrong in batch sort")
    CarbonProperties.getInstance().
      addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
        CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  }

  def getIndexfileCount(tableName: String, segmentNo: String = "0"): Int = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME,
      tableName
    )
    val segmentDir = carbonTable.getSemgentPath(segmentNo)
    new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir).size()
  }

  override def afterAll {
    dropTable
    new File(filePath).delete()
  }
}

