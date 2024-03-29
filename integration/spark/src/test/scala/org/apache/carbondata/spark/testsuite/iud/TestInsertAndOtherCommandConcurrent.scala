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

package org.apache.carbondata.spark.testsuite.iud

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{Callable, Executors, ExecutorService, Future}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.index.{IndexInputSplit, IndexMeta, Segment}
import org.apache.carbondata.core.index.dev.{IndexBuilder, IndexWriter}
import org.apache.carbondata.core.index.dev.cgindex.{CoarseGrainIndex, CoarseGrainIndexFactory}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, IndexSchema}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event

// This testsuite test insert and insert overwrite with other commands concurrently
class TestInsertAndOtherCommandConcurrent
  extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  private val executorService: ExecutorService = Executors.newFixedThreadPool(10)
  var testData: DataFrame = _

  override def beforeAll {
    dropTable()
    buildTestData()

    createTable("orders", testData.schema)
    createTable("orders_overwrite", testData.schema)
    sql(
      s"""
         | create index test
         | on table orders (o_name)
         | as '${classOf[WaitingIndexFactory].getName}'
       """.stripMargin)

    testData.write
      .format("carbondata")
      .option("tableName", "temp_table")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .saveAsTable("temp_table")

    sql(s"insert into orders select * from temp_table")
    sql(s"insert into orders_overwrite select * from temp_table")
  }

  private def buildTestData(): Unit = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    // Simulate data and write to table orders
    import sqlContext.implicits._

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    testData = sqlContext.sparkSession.sparkContext.parallelize(1 to 150000)
      .map(value => (value, new java.sql.Date(sdf.parse("2015-07-" + (value % 10 + 10)).getTime),
        "china", "aaa" + value, "phone" + 555 * value, "ASD" + (60000 + value), 14999 + value,
        "ordersTable" + value))
      .toDF("o_id", "o_date", "o_country", "o_name",
        "o_phonetype", "o_serialname", "o_salary", "o_comment")
  }

  private def createTable(tableName: String, schema: StructType): Unit = {
    val schemaString = schema.fields.map(x => x.name + " " + x.dataType.typeName).mkString(", ")
    sql(s"CREATE TABLE $tableName ($schemaString) stored as carbondata tblproperties" +
        s"('sort_scope'='local_sort','sort_columns'='o_country,o_name,o_phonetype,o_serialname," +
        s"o_comment')")
  }

  override def afterAll {
    executorService.shutdownNow()
    dropTable()
  }

  override def beforeEach(): Unit = {
    Global.loading = false
  }

  private def dropTable() = {
    sql("DROP TABLE IF EXISTS orders")
    sql("DROP TABLE IF EXISTS orders_overwrite")
  }

  // run the input SQL and block until it is running
  private def runSqlAsync(sql: String): Future[String] = {
    assert(!Global.loading)
    var count = 0
    val future = executorService.submit(
      new QueryTask(sql)
    )
    while (!Global.loading && count < 1000) {
      Thread.sleep(10)
      // to avoid dead loop in case WaitingIndexFactory is not invoked
      count += 1
    }
    future
  }

  // ----------- INSERT OVERWRITE --------------

  test("compaction should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException]{
      sql("alter table orders compact 'MINOR'")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, " +
      "compaction operation is not allowed"))
  }

  // block updating records from table which has index. see PR2483
  test("update should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("update orders set (o_country)=('newCountry') where o_country='china'").collect()
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, data update operation is not allowed"))
  }

  // block deleting records from table which has index. see PR2483
  test("delete should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("delete from orders where o_country='china'").collect()
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, data delete operation is not allowed"))
  }

  test("drop table should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("drop table if exists orders")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, drop table operation is not allowed"))
  }

  test("alter rename table should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("alter table orders rename to other")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, " +
      "alter table rename operation is not allowed"))
  }

  test("delete segment by id should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("DELETE FROM TABLE orders WHERE SEGMENT.ID IN (0)")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, " +
      "delete segment operation is not allowed"))
  }

  test("delete segment by date should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("DELETE FROM TABLE orders WHERE SEGMENT.STARTTIME BEFORE '2099-06-01 12:05:06' ")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, " +
      "delete segment operation is not allowed"))
  }

  test("clean file should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("clean files for table  orders")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, " +
      "clean file operation is not allowed"))
  }

  // ----------- INSERT  --------------

  test("compaction should allow if insert is in progress") {
    sql("drop table if exists t1")

    // number of segment is 1 after createTable
    createTable("t1", testData.schema)

    sql(
      s"""
         | create index dm_t1
         | on table t1 (o_name)
         | as '${classOf[WaitingIndexFactory].getName}'
       """.stripMargin)

    sql("insert into table t1 select * from orders_overwrite")
    Thread.sleep(1100)
    sql("insert into table t1 select * from orders_overwrite")
    Thread.sleep(1100)

    val future = runSqlAsync("insert into table t1 select * from orders_overwrite")
    sql("alter table t1 compact 'MAJOR'")
    assert(future.get.contains("PASS"))

    // all segments are compacted
    val segments = sql("show segments for table t1").collect()
    assert(segments.length == 5)

    sql("drop table t1")
  }

  // block updating records from table which has index. see PR2483
  test("update should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("update orders set (o_country)=('newCountry') where o_country='china'").collect()
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, data update operation is not allowed"))
  }

  // block deleting records from table which has index. see PR2483
  test("delete should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("delete from orders where o_country='china'").collect()
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, data delete operation is not allowed"))
  }

  test("drop table should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("drop table if exists orders")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, drop table operation is not allowed"))
  }

  test("alter rename table should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("alter table orders rename to other")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, " +
      "alter table rename operation is not allowed"))
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        sql(query).collect()
      } catch {
        case exception: Exception => LOGGER.error(exception.getMessage)
          result = "FAIL"
      }
      result
    }
  }

}

object Global {
  var loading = false
}

class WaitingIndexFactory(
    carbonTable: CarbonTable,
    indexSchema: IndexSchema) extends CoarseGrainIndexFactory(carbonTable, indexSchema) {
  // scalastyle:off ???
  override def fireEvent(event: Event): Unit = ???

  override def clear(): Unit = {}

  override def getIndexes(distributable: IndexInputSplit): util.List[CoarseGrainIndex] = ???

  override def getIndexes(segment: Segment): util.List[CoarseGrainIndex] = ???

  override def createWriter(segment: Segment,
      shardName: String,
      segmentProperties: SegmentProperties): IndexWriter = {
    new IndexWriter(carbonTable.getTablePath, indexSchema.getIndexName,
      carbonTable.getIndexedColumns(indexSchema.getIndexColumns), segment, shardName) {
      override def onPageAdded(blockletId: Int,
          pageId: Int,
          pageSize: Int,
          pages: Array[ColumnPage]): Unit = {}

      override def onBlockletEnd(blockletId: Int): Unit = { }

      override def onBlockEnd(blockId: String): Unit = { }

      override def onBlockletStart(blockletId: Int): Unit = { }

      override def onBlockStart(blockId: String): Unit = {
        // trigger the second SQL to execute
        Global.loading = true

        // wait for 1 second to let second SQL to finish
        Thread.sleep(1000)
      }

      override def finish(): Unit = {
        Global.loading = false
      }
    }
  }

  override def getMeta: IndexMeta = {
    new IndexMeta(carbonTable.getIndexedColumns(indexSchema
      .getIndexColumns), Seq(ExpressionType.EQUALS).asJava)
  }

  override def toDistributable(segmentId: Segment): util.List[IndexInputSplit] = {
    util.Collections.emptyList()
  }

  /**
   * delete index of the segment
   */
  override def deleteIndexData(segment: Segment): Unit = {

  }

  /**
   * delete index data if any
   */
  override def deleteIndexData(): Unit = {

  }

  /**
   * defines the features scopes for the index
   */
  override def willBecomeStale(operation: TableOperation): Boolean = {
    false
  }

  override def createBuilder(segment: Segment,
      shardName: String, segmentProperties: SegmentProperties): IndexBuilder = {
    ???
  }

  /**
   * Get the index for segmentId and partitionSpecs
   */
  override def getIndexes(segment: Segment,
      partitionLocations: util.Set[Path]): util.List[CoarseGrainIndex] = {
    ???
  }
  // scalastyle:on ???
}
