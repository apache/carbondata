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

package org.apache.carbondata.spark.testsuite.datamap

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.dev.DataMapWriter
import org.apache.carbondata.core.datamap.dev.cgdatamap.{CoarseGrainDataMap, CoarseGrainDataMapFactory}
import org.apache.carbondata.core.datamap.status.{DataMapStatus, DataMapStatusManager}
import org.apache.carbondata.core.datamap.{DataMapDistributable, DataMapMeta, Segment}
import org.apache.carbondata.core.datastore.page.ColumnPage
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.scan.filter.intf.ExpressionType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.Event

class TestDataMapStatus extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll: Unit = {
    drop
  }

  test("datamap status disable for new datamap") {
    sql("DROP TABLE IF EXISTS datamapstatustest")
    sql(
      """
        | CREATE TABLE datamapstatustest(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap on table datamapstatustest using '${classOf[TestDataMap].getName}' as select id,sum(age) from datamapstatustest group by id""".stripMargin)

    val details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap") && p.getStatus == DataMapStatus.DISABLED))
    sql("DROP TABLE IF EXISTS datamapstatustest")
  }

  test("datamap status disable after new load") {
    sql("DROP TABLE IF EXISTS datamapstatustest1")
    sql(
      """
        | CREATE TABLE datamapstatustest1(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap1 on table datamapstatustest1 using '${classOf[TestDataMap].getName}' as select id,sum(age) from datamapstatustest1 group by id""".stripMargin)

    var details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap1") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table datamapstatustest1")
    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap1") && p.getStatus == DataMapStatus.DISABLED))
    sql("DROP TABLE IF EXISTS datamapstatustest1")
  }

  test("datamap status with refresh datamap") {
    sql("DROP TABLE IF EXISTS datamapstatustest2")
    sql(
      """
        | CREATE TABLE datamapstatustest2(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap statusdatamap2 on table datamapstatustest2 using '${classOf[TestDataMap].getName}' as select id,sum(age) from datamapstatustest1 group by id""".stripMargin)

    var details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap2") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table datamapstatustest2")
    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap2") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"refresh datamap statusdatamap2 on table datamapstatustest2")

    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap2") && p.getStatus == DataMapStatus.ENABLED))

    sql("DROP TABLE IF EXISTS datamapstatustest2")
  }

  test("datamap create without on table test") {
    sql("DROP TABLE IF EXISTS datamapstatustest3")
    sql(
      """
        | CREATE TABLE datamapstatustest3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    intercept[MalformedDataMapCommandException] {
      sql(
        s"""create datamap statusdatamap3 using '${
          classOf[TestDataMap]
            .getName
        }' as select id,sum(age) from datamapstatustest3 group by id""".stripMargin)
    }

    sql(
      s"""create datamap statusdatamap3 on table datamapstatustest3 using '${
        classOf[TestDataMap]
          .getName
      }' as select id,sum(age) from datamapstatustest3 group by id""".stripMargin)

    var details = DataMapStatusManager.readDataMapStatusDetails()

    assert(details.length == 1)

    assert(details.exists(p => p.getDataMapName.equals("statusdatamap3") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"LOAD DATA LOCAL INPATH '$testData' into table datamapstatustest3")
    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap3") && p.getStatus == DataMapStatus.DISABLED))

    sql(s"refresh datamap statusdatamap3")

    details = DataMapStatusManager.readDataMapStatusDetails()
    assert(details.length == 1)
    assert(details.exists(p => p.getDataMapName.equals("statusdatamap3") && p.getStatus == DataMapStatus.ENABLED))

    checkExistence(sql(s"show datamap"), true, "statusdatamap3")

    sql("DROP TABLE IF EXISTS datamapstatustest3")
  }

  override def afterAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
      CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    drop
  }

  private def drop = {
    sql("drop table if exists datamapstatustest")
    sql("drop table if exists datamapshowtest")
    sql("drop table if exists datamapstatustest1")
    sql("drop table if exists datamapstatustest2")
  }
}

class TestDataMap() extends CoarseGrainDataMapFactory {

  private var identifier: AbsoluteTableIdentifier = _

  override def fireEvent(event: Event): Unit = ???

  override def clear(segmentId: Segment): Unit = {}

  override def clear(): Unit = {}

  override def getDataMaps(distributable: DataMapDistributable,
      readCommitted: ReadCommittedScope): util.List[CoarseGrainDataMap] = {
    ???
  }

  override def getDataMaps(segment: Segment,
      readCommitted: ReadCommittedScope): util.List[CoarseGrainDataMap] = {
    ???
  }

  override def createWriter(segment: Segment, writeDirectoryPath: String): DataMapWriter = {
    new DataMapWriter(identifier, segment, writeDirectoryPath) {
      override def onPageAdded(blockletId: Int, pageId: Int, pages: Array[ColumnPage]): Unit = { }

      override def onBlockletEnd(blockletId: Int): Unit = { }

      override def onBlockEnd(blockId: String): Unit = { }

      override def onBlockletStart(blockletId: Int): Unit = { }

      override def onBlockStart(blockId: String, taskId: Long): Unit = {
        // trigger the second SQL to execute
      }

      override def finish(): Unit = {

      }
    }
  }

  override def getMeta: DataMapMeta = new DataMapMeta(List("id").asJava, Seq(ExpressionType.EQUALS).asJava)

  override def toDistributable(segmentId: Segment): util.List[DataMapDistributable] = ???

  override def init(carbonTable: CarbonTable, dataMapSchema: DataMapSchema): Unit = {
    this.identifier = carbonTable.getAbsoluteTableIdentifier
  }
}
