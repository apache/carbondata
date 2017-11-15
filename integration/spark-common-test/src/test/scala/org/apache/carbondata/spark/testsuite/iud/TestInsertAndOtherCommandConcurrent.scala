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
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import scala.collection.JavaConverters._

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.dev.{DataMap, DataMapFactory}
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.dev.cgdatamap.{AbstractCoarseGrainDataMap, AbstractCoarseGrainDataMapFactory}
import org.apache.carbondata.core.datamap.dev.{AbstractDataMapWriter, DataMap}
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, DataMapStoreManager}
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event
import org.apache.carbondata.spark.exception.ConcurrentOperationException
import org.apache.carbondata.spark.testsuite.datamap.C2DataMapFactory

// This testsuite test insert and insert overwrite with other commands concurrently
class TestInsertAndOtherCommandConcurrent extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  private val executorService: ExecutorService = Executors.newFixedThreadPool(10)
  var df: DataFrame = _

  override def beforeAll {
    dropTable()
    buildTestData()

    // register hook to the table to sleep, thus the other command will be executed
    DataMapStoreManager.getInstance().createAndRegisterDataMap(
      AbsoluteTableIdentifier.from(storeLocation + "/orders", "default", "orders"),
      classOf[WaitingDataMap].getName,
      "test")
  }

  private def buildTestData(): Unit = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    // Simulate data and write to table orders
    import sqlContext.implicits._

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    df = sqlContext.sparkSession.sparkContext.parallelize(1 to 150000)
      .map(value => (value, new java.sql.Date(sdf.parse("2015-07-" + (value % 10 + 10)).getTime),
        "china", "aaa" + value, "phone" + 555 * value, "ASD" + (60000 + value), 14999 + value,
        "ordersTable" + value))
      .toDF("o_id", "o_date", "o_country", "o_name",
        "o_phonetype", "o_serialname", "o_salary", "o_comment")
    createTable("orders")
    createTable("orders_overwrite")
  }

  private def createTable(tableName: String): Unit = {
    df.write
      .format("carbondata")
      .option("tableName", tableName)
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()
  }

  override def afterAll {
    executorService.shutdownNow()
    dropTable()
  }

  override def beforeEach(): Unit = {
    Global.overwriteRunning = false
  }

  private def dropTable() = {
    sql("DROP TABLE IF EXISTS orders")
    sql("DROP TABLE IF EXISTS orders_overwrite")
  }

  // run the input SQL and block until it is running
  private def runSqlAsync(sql: String): Future[String] = {
    assert(!Global.overwriteRunning)
    var count = 0
    val future = executorService.submit(
      new QueryTask(sql)
    )
    while (!Global.overwriteRunning && count < 1000) {
      Thread.sleep(10)
      // to avoid dead loop in case WaitingDataMap is not invoked
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
      "loading is in progress for table default.orders, compaction operation is not allowed"))
  }

  test("update should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("update orders set (o_country)=('newCountry') where o_country='china'").show
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, data update operation is not allowed"))
  }

  test("delete should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("delete from orders where o_country='china'").show
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
      "loading is in progress for table default.orders, alter table rename operation is not allowed"))
  }

  test("delete segment by id should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("DELETE FROM TABLE orders WHERE SEGMENT.ID IN (0)")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, delete segment operation is not allowed"))
  }

  test("delete segment by date should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("DELETE FROM TABLE orders WHERE SEGMENT.STARTTIME BEFORE '2099-06-01 12:05:06' ")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, delete segment operation is not allowed"))
  }

  test("clean file should fail if insert overwrite is in progress") {
    val future = runSqlAsync("insert overwrite table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("clean files for table  orders")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "insert overwrite is in progress for table default.orders, clean file operation is not allowed"))
  }

  // ----------- INSERT  --------------

  test("compaction should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException]{
      sql("alter table orders compact 'MINOR'")
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, compaction operation is not allowed"))
  }

  test("update should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("update orders set (o_country)=('newCountry') where o_country='china'").show
    }
    assert(future.get.contains("PASS"))
    assert(ex.getMessage.contains(
      "loading is in progress for table default.orders, data update operation is not allowed"))
  }

  test("delete should fail if insert is in progress") {
    val future = runSqlAsync("insert into table orders select * from orders_overwrite")
    val ex = intercept[ConcurrentOperationException] {
      sql("delete from orders where o_country='china'").show
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
      "loading is in progress for table default.orders, alter table rename operation is not allowed"))
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
  var overwriteRunning = false
}

class WaitingDataMap() extends AbstractCoarseGrainDataMapFactory {

  override def init(identifier: AbsoluteTableIdentifier, dataMapName: String): Unit = { }

  override def fireEvent(event: Event): Unit = ???

  override def clear(segmentId: Segment): Unit = {}

  override def clear(): Unit = {}

  override def getDataMaps(distributable: DataMapDistributable): java.util.List[AbstractCoarseGrainDataMap] = ???

  override def getDataMaps(segmentId: Segment): util.List[DataMap] = ???

  override def createWriter(segmentId: Segment): AbstractDataMapWriter = {
    new AbstractDataMapWriter {
      override def onPageAdded(blockletId: Int, pageId: Int, pages: Array[ColumnPage]): Unit = { }

      override def onBlockletEnd(blockletId: Int): Unit = { }

      override def onBlockEnd(blockId: String): Unit = { }

      override def onBlockletStart(blockletId: Int): Unit = { }

      override def onBlockStart(blockId: String): Unit = {
        // trigger the second SQL to execute
        Global.overwriteRunning = true

        // wait for 1 second to let second SQL to finish
        Thread.sleep(1000)
      }

      override def finish(): Unit = {

      }
    }
  }

  override def getMeta: DataMapMeta = new DataMapMeta(List("o_country").asJava, Seq(ExpressionType.EQUALS).asJava)

  override def toDistributable(segmentId: Segment): util.List[DataMapDistributable] = ???
}