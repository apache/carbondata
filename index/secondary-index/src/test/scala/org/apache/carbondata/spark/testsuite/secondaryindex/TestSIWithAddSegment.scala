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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.sdk.file.{CarbonSchemaReader, CarbonWriterBuilder, Field, Schema}

@Ignore
class TestSIWithAddSegment extends QueryTest with BeforeAndAfterAll {

  val newSegmentPath: String = warehouse + "/newsegment/"

  override protected def beforeAll(): Unit = {
    dropTables()
    FileFactory.getCarbonFile(newSegmentPath).delete()
    sql("create table maintable(a string, b int, c string) stored as carbondata")
    sql("insert into maintable select 'k',1,'k'")
    sql("insert into maintable select 'l',2,'l'")
    sql("CREATE INDEX maintable_si  on table maintable (c) as 'carbondata'")
    val carbonTable = CarbonEnv.getCarbonTable(None, "maintable")(sqlContext.sparkSession)
    val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, "0")
    val schema = CarbonSchemaReader.readSchema(segmentPath).asOriginOrder()
    val writer = new CarbonWriterBuilder()
      .outputPath(newSegmentPath).withCsvInput(schema).writtenBy("TestSIWithAddSegment").build()
    writer.write(Array[String]("m", "3", "m"))
    writer.close()
    sql(s"alter table maintable add segment options('path'='${ newSegmentPath }', " +
        s"'format'='carbon')")
  }

  override protected def afterAll(): Unit = {
    dropTables()
    FileFactory.getCarbonFile(newSegmentPath).delete()
  }

  private def dropTables(): Unit = {
    sql("drop table if exists maintable")
    sql("drop table if exists maintable1")
  }

  test("test if the query hits SI after adding a segment to the main table") {
    val d = sql("select * from maintable where c = 'm'")
    assert(d.queryExecution.executedPlan.isInstanceOf[BroadCastSIFilterPushJoin])
  }

  ignore("compare results of SI and NI after adding segments") {
    val siResult = sql("select * from maintable where c = 'm'")
    val niResult = sql("select * from maintable where ni(c = 'm')")
    assert(!niResult.queryExecution.executedPlan.isInstanceOf[BroadCastSIFilterPushJoin])
    checkAnswer(siResult, niResult)
  }

  ignore("test SI creation after adding segments") {
    sql("create table maintable1(a string, b int, c string) stored as carbondata")
    sql("insert into maintable1 select 'k',1,'k'")
    sql("insert into maintable1 select 'l',2,'l'")
    val carbonTable = CarbonEnv.getCarbonTable(None, "maintable1")(sqlContext.sparkSession)
    val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, "0")
    val schema = CarbonSchemaReader.readSchema(segmentPath).asOriginOrder()
    val writer = new CarbonWriterBuilder()
      .outputPath(newSegmentPath).withCsvInput(schema).writtenBy("TestSIWithAddSegment").build()
    writer.write(Array[String]("m", "3", "m"))
    writer.close()
    sql(s"alter table maintable1 add segment options('path'='${ newSegmentPath }', " +
        s"'format'='carbon')")
    sql("CREATE INDEX maintable1_si  on table maintable1 (c) as 'carbondata'")
    assert(sql("show segments for table maintable1_si").collect().length ==
           sql("show segments for table maintable1").collect().length)
    val siResult = sql("select * from maintable1 where c = 'm'")
    val niResult = sql("select * from maintable1 where ni(c = 'm')")
    assert(!niResult.queryExecution.executedPlan.isInstanceOf[BroadCastSIFilterPushJoin])
    checkAnswer(siResult, niResult)
  }

  ignore("test query on SI with all external segments") {
    sql("drop table if exists maintable1")
    sql("create table maintable1(a string, b int, c string) stored as carbondata")
    sql("CREATE INDEX maintable1_si  on table maintable1 (c) as 'carbondata'")
    val fields = Array(new Field("a", "string"), new Field("b", "int"), new Field("c", "string"))
    val writer = new CarbonWriterBuilder()
      .outputPath(newSegmentPath)
      .withCsvInput(new Schema(fields))
      .writtenBy("TestSIWithAddSegment")
      .build()
    writer.write(Array[String]("m", "3", "m"))
    writer.close()
    sql(s"alter table maintable1 add segment options('path'='${ newSegmentPath }', " +
        s"'format'='carbon')")
    val siResult = sql("select * from maintable1 where c = 'm'")
    val niResult = sql("select * from maintable1 where ni(c = 'm')")
    checkAnswer(siResult, niResult)
  }
}
