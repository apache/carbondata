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

package org.apache.carbondata.spark.testsuite.dataretention

import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import scala.collection.JavaConverters._

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.dev.DataMapWriter
import org.apache.carbondata.core.datamap.dev.cgdatamap.{CoarseGrainDataMap, CoarseGrainDataMapFactory}
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, Segment}
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event


/**
 * This class contains DataRetention concurrency test cases
 */
class DataRetentionConcurrencyTestCase extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  private val executorService: ExecutorService = Executors.newFixedThreadPool(10)

  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK, "1")


    sql("drop table if exists concurrent")

    sql(
      "create table concurrent (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'"

    )
    sql(
      s"""
         | create datamap test on table concurrent
         | using '${ classOf[WaitingDataMap].getName }'
         | as select count(a) from hiveMetaStoreTable_1")
       """.stripMargin)

  }

  override def afterAll {
    sql("drop table if exists concurrent")
  }

  override def beforeEach(): Unit = {
    Global.overwriteRunning = false
  }

  ignore("DataRetention_Concurrency_load_id") {

    val tasks = new util.ArrayList[Callable[String]]()
    tasks
      .add(new QueryTask(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
                         s"concurrent OPTIONS('DELIMITER' =  ',')"))
    tasks.add(new QueryTask("delete from table concurrent where segment.id in (0)"))
    tasks.add(new QueryTask("clean files for table concurrent"))
    val futures = executorService.invokeAll(tasks)
    val results = futures.asScala.map(_.get)
    for (i <- results.indices) {
      assert("PASS".equals(results(i)))
    }
  }

  test("DataRetention_Concurrency_load_date") {
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE concurrent " +
      "OPTIONS('DELIMITER' =  ',')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE concurrent " +
      "OPTIONS('DELIMITER' =  ',')")

    Global.overwriteRunning = false
    val future1 = runSqlAsync(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
      s"concurrent OPTIONS('DELIMITER' =  ',')")

    Global.overwriteRunning = false

    val future2 = runSqlAsync(
      "delete from table concurrent where segment.starttime before '2099-01-01 00:00:00'")

    Global.overwriteRunning = false

    val future3 = runSqlAsync("clean files for table concurrent")

    val listOfFutures = List(future1, future2, future3)
    listOfFutures.foreach {
      futureValue => assert(futureValue.get().equals("PASS"))
    }

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

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        LOGGER.info("Executing :" + Thread.currentThread().getName)
        sql(query)
      } catch {
        case ex: Exception =>
          LOGGER.info(s"Executing Query $query")
          ex.printStackTrace()
          result = "FAIL"
      }
      result
    }
  }

}

object Global {
  var overwriteRunning = false
}

class WaitingDataMap() extends CoarseGrainDataMapFactory {

  private var identifier: AbsoluteTableIdentifier = _

  override def fireEvent(event: Event): Unit = ???

  override def clear(segmentId: Segment): Unit = {}

  override def clear(): Unit = {}

  override def getDataMaps(distributable: DataMapDistributable): util.List[CoarseGrainDataMap] = ???

  override def getDataMaps(segment: Segment): util.List[CoarseGrainDataMap] = ???

  override def createWriter(segment: Segment, writeDirectoryPath: String): DataMapWriter = {
    new DataMapWriter(identifier, segment, writeDirectoryPath) {
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

  override def getMeta: DataMapMeta = new DataMapMeta(List("id").asJava, Seq(ExpressionType.EQUALS).asJava)

  override def toDistributable(segmentId: Segment): util.List[DataMapDistributable] = ???

  override def init(identifier: AbsoluteTableIdentifier,
      dataMapSchema: DataMapSchema): Unit = {
    this.identifier = identifier
  }
}