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

package org.apache.carbondata.spark.testsuite.merge

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.LocalDateTime

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.mutation.merge._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Test Class for carbon merge api
 */

class MergeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

  }

  def generateData(numOrders: Int = 10): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(1 to numOrders, 4)
      .map { x => ("id" + x, s"order$x", s"customer$x", x * 10, x * 75, 1)
      }.toDF("id", "name", "c_name", "quantity", "price", "state")
  }

  def generateFullCDC(
      numOrders: Int,
      numUpdatedOrders: Int,
      newState: Int,
      oldState: Int,
      numNewOrders: Int
  ): DataFrame = {
    import sqlContext.implicits._
    val ds1 = sqlContext.sparkContext.parallelize(numNewOrders + 1 to (numOrders), 4)
      .map { x =>
        if (x <= numNewOrders + numUpdatedOrders) {
          ("id" + x, s"order$x", s"customer$x", x * 10, x * 75, newState)
        } else {
          ("id" + x, s"order$x", s"customer$x", x * 10, x * 75, oldState)
        }
      }.toDF("id", "name", "c_name", "quantity", "price", "state")
    val ds2 = sqlContext.sparkContext.parallelize(1 to numNewOrders, 4)
      .map { x => ("newid" + x, s"order$x", s"customer$x", x * 10, x * 75, oldState)
      }.toDS().toDF()
    ds1.union(ds2)
  }

  private def initialize = {
    val initframe = generateData(10)
    initframe.write
      .format("carbondata")
      .option("tableName", "order")
      .mode(SaveMode.Overwrite)
      .save()

    val dwframe = sqlContext.read.format("carbondata").option("tableName", "order").load()
    val dwSelframe = dwframe.as("A")

    val odsframe = generateFullCDC(10, 2, 2, 1, 2).as("B")
    (dwSelframe, odsframe)
  }

  private def initializeWithBucketing(bucketingColumns: Seq[String]) = {
    sql(s"create table order(id string, name string, c_name string, quantity int, price int, " +
        s"state int) stored as carbondata tblproperties('BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='${
      bucketingColumns.mkString(",")
    }')")
    initialize
  }

  private def initializeGloabalSort = {
    val initframe = generateData(10)
    initframe.write
      .format("carbondata")
      .option("tableName", "order")
      .option("sort_scope", "global_sort")
      .option("sort_columns", "id")
      .mode(SaveMode.Overwrite)
      .save()

    val dwframe = sqlContext.read.format("carbondata").option("tableName", "order").load()
    val dwSelframe = dwframe.as("A")

    val odsframe = generateFullCDC(10, 2, 2, 1, 2).as("B")
    (dwSelframe, odsframe)
  }

  private def initializeLocalSort = {
    val initframe = generateData(10)
    initframe.write
      .format("carbondata")
      .option("tableName", "order")
      .option("sort_scope", "local_sort")
      .option("sort_columns", "id")
      .mode(SaveMode.Overwrite)
      .save()

    val dwframe = sqlContext.read.format("carbondata").option("tableName", "order").load()
    val dwSelframe = dwframe.as("A")

    val odsframe = generateFullCDC(10, 2, 2, 1, 2).as("B")
    (dwSelframe, odsframe)
  }

  private def initializeNoSortWithSortColumns = {
    val initframe = generateData(10)
    initframe.write
      .format("carbondata")
      .option("tableName", "order")
      .option("sort_scope", "no_sort")
      .option("sort_columns", "id")
      .mode(SaveMode.Overwrite)
      .save()

    val dwframe = sqlContext.read.format("carbondata").option("tableName", "order").load()
    val dwSelframe = dwframe.as("A")

    val odsframe = generateFullCDC(10, 2, 2, 1, 2).as("B")
    (dwSelframe, odsframe)
  }

  private def initializePartition = {
    val initframe = generateData(10)
    initframe.write
      .format("carbondata")
      .option("tableName", "order")
      .option("partitionColumns", "c_name")
      .mode(SaveMode.Overwrite)
      .save()

    val dwframe = sqlContext.read.format("carbondata").option("tableName", "order").load()
    val dwSelframe = dwframe.as("A")

    val odsframe = generateFullCDC(10, 2, 2, 1, 2).as("B")
    (dwSelframe, odsframe)
  }

  private def initializeWithDateTimeFormat = {
    import sqlContext.implicits._
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val initframe = sqlContext.sparkContext.parallelize(1 to 10, 4)
      .map { x =>
        ("id" + x, s"order$x", s"customer$x", x * 10, x * 75, 1, new Date(sdf
          .parse("2015-07-23").getTime), Timestamp.valueOf("2015-03-03 12:25:03.205"))
      }.toDF("id", "name", "c_name", "quantity", "price", "state", "date", "time")
    val loadframe = sqlContext.sparkContext.parallelize(11 to 12, 4)
      .map { x =>
        ("id" + x, s"order$x", s"customer$x", x * 10, x * 75, 1, new Date(sdf
          .parse("2020-07-23").getTime), Timestamp.valueOf("2020-04-04 09:40:05.205"))
      }.toDF("id", "name", "c_name", "quantity", "price", "state", "date", "time")
    // setting date and timestampformat table level
    initframe.write
      .format("carbondata")
      .option("tableName", "order")
      .option("dateformat", "yyyy-MM-dd")
      .option("timestampformat", "yyyy-MM-dd HH:mm")
      .mode(SaveMode.Overwrite)
      .save()
    // setting date and timestampformat for another load option
    loadframe.write
      .format("carbondata")
      .option("tableName", "order")
      .option("dateformat", "yyyy-MM")
      .option("timestampformat", "yyyy-MM-dd HH:mm:ss.SSS")
      .mode(SaveMode.Append)
      .save()
    val dwframe = sqlContext.read.format("carbondata").option("tableName", "order").load()
    val dwSelframe = dwframe.as("A")

    val odsframe = sqlContext.sparkContext.parallelize(1 to 4, 4)
      .map { x =>
        ("id" + x, s"order$x", s"customer$x", x * 10, x * 75, 2,
          new Date(sdf.parse("2015-07-23").getTime), Timestamp.valueOf("2015-05-23 10:30:30"))
      }.toDS().toDF("id", "name", "c_name", "quantity", "price", "state", "date", "time").as("B")

    (dwSelframe, odsframe)
  }

  test("test basic merge update with all mappings") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    val updateMap = Map("id" -> "A.id",
      "name" -> "B.name",
      "c_name" -> "B.c_name",
      "quantity" -> "B.quantity",
      "price" -> "B.price",
      "state" -> "B.state").asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).whenMatched(
      col("A.state") =!= col("B.state")).updateExpr(updateMap).execute()
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test basic merge update with few mappings") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    val updateMap = Map(col("id") -> col("A.id"),
      col("state") -> col("B.state")).asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, "A.id=B.id")
      .whenMatched("A.state <> B.state")
      .updateExpr(updateMap)
      .execute()

    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test basic merge update with few mappings and expressions") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    val updateMap = Map("id" -> "A.id",
      "price" -> "B.price * 100",
      "state" -> "B.state").asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).whenMatched(
      col("A.state") =!= col("B.state")).updateExpr(updateMap).execute()
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select price from order where where state = 2"), Seq(Row(22500), Row(30000)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test basic merge into the globalsort table") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initializeGloabalSort

    val updateMap = Map("id" -> "A.id",
      "name" -> "B.name",
      "c_name" -> "B.c_name",
      "quantity" -> "B.quantity",
      "price" -> "B.price",
      "state" -> "B.state").asInstanceOf[Map[Any, Any]]
    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).whenMatched(
      col("A.state") =!= col("B.state")).updateExpr(updateMap).execute()
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test basic merge into the localsort table") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initializeLocalSort

    val updateMap = Map("id" -> "A.id",
      "name" -> "B.name",
      "c_name" -> "B.c_name",
      "quantity" -> "B.quantity",
      "price" -> "B.price",
      "state" -> "B.state").asInstanceOf[Map[Any, Any]]
    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).whenMatched(
      col("A.state") =!= col("B.state")).updateExpr(updateMap).execute()
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test basic merge into the nosort table with sortcolumns") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initializeNoSortWithSortColumns

    val updateMap = Map("id" -> "A.id",
      "name" -> "B.name",
      "c_name" -> "B.c_name",
      "quantity" -> "B.quantity",
      "price" -> "B.price",
      "state" -> "B.state").asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).whenMatched(
      col("A.state") =!= col("B.state")).updateExpr(updateMap).execute()
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test basic merge update with few mappings with out condition") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    val updateMap = Map(col("id") -> col("A.id"),
      col("state") -> col("B.state")).asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id")))
      .whenMatched()
      .updateExpr(updateMap)
      .execute()
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test merge insert with condition") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      "c_name" -> col("B.c_name"),
      col("quantity") -> "B.quantity",
      col("price") -> col("B.price"),
      col("state") -> col("B.state")).asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).
      whenNotMatched(col("A.id").isNull.and(col("B.id").isNotNull)).
      insertExpr(insertMap).execute()

    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
  }

  test("test merge update and insert with out condition") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> col("B.price"),
      col("state") -> col("B.state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatched().addAction(InsertAction(insertMap)))

    val st = System.currentTimeMillis()
    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test merge update and insert with condition") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> col("B.price"),
      col("state") -> col("B.state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatched(Some(col("A.id").isNull.and(col("B.id").isNotNull)))
      .addAction(InsertAction(insertMap)))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order"), Seq(Row(12)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test merge update and insert with condition and expression") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatched(Some(col("A.id").isNull.and(col("B.id").isNotNull)))
      .addAction(InsertAction(insertMap)))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order"), Seq(Row(12)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
  }

  test("test merge with only delete action") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget().addAction(DeleteAction()))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order"), Seq(Row(8)))
  }

  test("test merge update and delete action") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget().addAction(DeleteAction()))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order"), Seq(Row(8)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
  }

  test("test merge update and insert with condition and expression and delete action") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatched().addAction(InsertAction(insertMap)))
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget().addAction(DeleteAction()))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
  }

  test("test merge update with insert, insert with condition and expression " +
       "and delete with insert action") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> "B.price + 1",
      col("state") -> col("B.state")).asInstanceOf[Map[Any, Any]]

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state")).asInstanceOf[Map[Any, Any]]

    val insertMap_u = Map(col("id") -> col("A.id"),
      col("name") -> col("A.name"),
      col("c_name") -> lit("insert"),
      col("quantity") -> col("A.quantity"),
      col("price") -> expr("A.price"),
      col("state") -> col("A.state")).asInstanceOf[Map[Any, Any]]

    val insertMap_d = Map(col("id") -> col("A.id"),
      col("name") -> col("A.name"),
      col("c_name") -> lit("delete"),
      col("quantity") -> col("A.quantity"),
      col("price") -> expr("A.price"),
      col("state") -> col("A.state")).asInstanceOf[Map[Any, Any]]

    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).
      whenMatched(col("A.state") =!= col("B.state")).
      updateExpr(updateMap).insertExpr(insertMap_u).
      whenNotMatched().
      insertExpr(insertMap).
      whenNotMatchedAndExistsOnlyOnTarget().
      delete().
      insertExpr(insertMap_d).
      execute()
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where c_name = 'delete'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order where c_name = 'insert'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order"), Seq(Row(14)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
  }

  test("test merge update with insert, insert with condition and expression " +
       "and delete with insert history action") {
    sql("drop table if exists order")
    sql("drop table if exists order_hist")
    sql("create table order_hist(id string, name string, c_name string, quantity int, " +
        "price int, state int) stored as carbondata")
    val (dwSelframe, odsframe) = initialize

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state"))

    val insertMap_u = Map(col("id") -> col("id"),
      col("name") -> col("name"),
      col("c_name") -> lit("insert"),
      col("quantity") -> col("quantity"),
      col("price") -> expr("price"),
      col("state") -> col("state"))

    val insertMap_d = Map(col("id") -> col("id"),
      col("name") -> col("name"),
      col("c_name") -> lit("delete"),
      col("quantity") -> col("quantity"),
      col("price") -> expr("price"),
      col("state") -> col("state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap))
      .addAction(InsertInHistoryTableAction(insertMap_u, TableIdentifier("order_hist"))))
    matches ++= Seq(WhenNotMatched().addAction(InsertAction(insertMap)))
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget()
      .addAction(DeleteAction())
      .addAction(InsertInHistoryTableAction(insertMap_d, TableIdentifier("order_hist"))))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 3)
    checkAnswer(sql("select count(*) from order"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
    checkAnswer(sql("select count(*) from order_hist where c_name = 'delete'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order_hist where c_name = 'insert'"), Seq(Row(2)))
  }

  test("test merge update with insert, insert with condition and expression " +
       "and delete with insert history action with partition") {
    sql("drop table if exists order")
    sql("drop table if exists order_hist")
    sql("create table order_hist(id string, name string, quantity int, price int, " +
        "state int) PARTITIONED BY (c_name String) STORED AS carbondata")
    val (dwSelframe, odsframe) = initializePartition

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state"))

    val insertMap_u = Map(col("id") -> col("id"),
      col("name") -> col("name"),
      col("c_name") -> lit("insert"),
      col("quantity") -> col("quantity"),
      col("price") -> expr("price"),
      col("state") -> col("state"))

    val insertMap_d = Map(col("id") -> col("id"),
      col("name") -> col("name"),
      col("c_name") -> lit("delete"),
      col("quantity") -> col("quantity"),
      col("price") -> expr("price"),
      col("state") -> col("state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap))
      .addAction(InsertInHistoryTableAction(insertMap_u, TableIdentifier("order_hist"))))
    matches ++= Seq(WhenNotMatched().addAction(InsertAction(insertMap)))
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget()
      .addAction(DeleteAction())
      .addAction(InsertInHistoryTableAction(insertMap_d, TableIdentifier("order_hist"))))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    checkAnswer(sql("select count(*) from order"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
    checkAnswer(sql("select count(*) from order_hist where c_name = 'delete'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order_hist where c_name = 'insert'"), Seq(Row(2)))
  }

  test("check the scd ") {
    sql("drop table if exists customers")

    val initframe = sqlContext.sparkSession.createDataFrame(
      Seq(Row(1, "old address for 1", false, null, Date.valueOf("2018-02-01")),
        Row(1, "current address for 1", true, Date.valueOf("2018-02-01"), null),
        Row(2, "current address for 2", true, Date.valueOf("2018-02-01"), null),
        Row(3, "current address for 3", true, Date.valueOf("2018-02-01"), null)).asJava,
      StructType(Seq(StructField("customerId", IntegerType),
        StructField("address", StringType),
        StructField("current", BooleanType),
        StructField("effectiveDate", DateType),
        StructField("endDate", DateType))))
    initframe.printSchema()
    initframe.write
      .format("carbondata")
      .option("tableName", "customers")
      .mode(SaveMode.Overwrite)
      .save()
    var customer = sqlContext.read.format("carbondata").option("tableName", "customers").load()
    customer = customer.as("A")
    var updates = sqlContext.sparkSession.createDataFrame(
      Seq(Row(1, "new address for 1", Date.valueOf("2018-03-03")),
        // new address same as current address for customer 3
        Row(3, "current address for 3", Date.valueOf("2018-04-04")),
        Row(4, "new address for 4", Date.valueOf("2018-04-04"))).asJava,
      StructType(Seq(StructField("customerId", IntegerType),
        StructField("address", StringType),
        StructField("effectiveDate", DateType))))
    updates = updates.as("B")

    val updateMap = Map(col("current") -> lit(false),
      col("endDate") -> col("B.effectiveDate")).asInstanceOf[Map[Any, Any]]

    val insertMap = Map(col("customerId") -> col("B.customerId"),
      col("address") -> col("B.address"),
      col("current") -> lit(true),
      col("effectiveDate") -> col("B.effectiveDate"),
      col("endDate") -> lit(null)).asInstanceOf[Map[Any, Any]]

    val insertMap_u = Map(col("customerId") -> col("B.customerId"),
      col("address") -> col("B.address"),
      col("current") -> lit(true),
      col("effectiveDate") -> col("B.effectiveDate"),
      col("endDate") -> lit(null)).asInstanceOf[Map[Any, Any]]

    customer.merge(updates, "A.customerId=B.customerId").
      whenMatched((col("A.address") =!= col("B.address")).and(col("A.current").equalTo(lit(true)))).
      updateExpr(updateMap).
      insertExpr(insertMap_u).
      whenNotMatched(col("A.customerId").isNull.and(col("B.customerId").isNotNull)).
      insertExpr(insertMap).
      execute()

    assert(getDeleteDeltaFileCount("customers", "0") == 1)
    checkAnswer(sql("select count(*) from customers"), Seq(Row(6)))
    checkAnswer(sql("select count(*) from customers where current='true'"), Seq(Row(4)))
    checkAnswer(sql("select count(*) from customers " +
                    "where effectivedate is not null and enddate is not null"), Seq(Row(1)))

  }

  test("check the cdc with partition") {
    sql("drop table if exists target")

    val initframe = sqlContext.sparkSession.createDataFrame(Seq(
      Row("a", "0"),
      Row("b", "1"),
      Row("c", "2"),
      Row("d", "3")
    ).asJava, StructType(Seq(StructField("key", StringType), StructField("value", StringType))))

    initframe.write
      .format("carbondata")
      .option("tableName", "target")
      .option("partitionColumns", "value")
      .mode(SaveMode.Overwrite)
      .save()
    val target = sqlContext.read.format("carbondata").option("tableName", "target").load()
    var cdc =
      sqlContext.sparkSession.createDataFrame(Seq(
        Row("a", "10", false, 0),
        Row("a", null, true, 1),   // a was updated and then deleted
        Row("b", null, true, 2),   // b was just deleted once
        Row("c", null, true, 3),   // c was deleted and then updated twice
        Row("c", "20", false, 4),
        Row("c", "200", false, 5),
        Row("e", "100", false, 6)  // new key
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("newValue", StringType),
          StructField("deleted", BooleanType), StructField("time", IntegerType))))
    cdc.createOrReplaceTempView("changes")

    cdc = sql("SELECT key, latest.newValue as newValue, latest.deleted as deleted FROM ( SELECT " +
              "key, max(struct(time, newValue, deleted)) as latest FROM changes GROUP BY key)")

    val updateMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    val insertMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    target.as("A").merge(cdc.as("B"), "A.key=B.key").
      whenMatched("B.deleted=false").
      updateExpr(updateMap).
      whenNotMatched("B.deleted=false").
      insertExpr(insertMap).
      whenMatched("B.deleted=true").
      delete().execute()
    assert(getDeleteDeltaFileCount("target", "0") == 0)
    checkAnswer(sql("select count(*) from target"), Seq(Row(3)))
    checkAnswer(sql("select * from target order by key"),
      Seq(Row("c", "200"), Row("d", "3"), Row("e", "100")))

    // insert overwrite a partition. make sure the merge executed before still works.
    sql(
      """insert overwrite table target
        | partition (value=3)
        | select * from target where value = 100""".stripMargin)
    checkAnswer(sql("select * from target order by key"),
      Seq(Row("c", "200"), Row("e", "100"), Row("e", "3")))
    sql("""alter table target drop partition (value=3)""")
    checkAnswer(sql("select * from target order by key"),
      Seq(Row("c", "200"), Row("e", "100")))
  }

  test("check the cdc ") {
    sql("drop table if exists target")

    val initframe = sqlContext.sparkSession.createDataFrame(Seq(
      Row("a", "0"),
      Row("b", "1"),
      Row("c", "2"),
      Row("d", "3")
    ).asJava, StructType(Seq(StructField("key", StringType), StructField("value", StringType))))

    initframe.write
      .format("carbondata")
      .option("tableName", "target")
      .mode(SaveMode.Overwrite)
      .save()
    val target = sqlContext.read.format("carbondata").option("tableName", "target").load()
    var cdc =
      sqlContext.sparkSession.createDataFrame(Seq(
        Row("a", "10", false, 0),
        Row("a", null, true, 1),   // a was updated and then deleted
        Row("b", null, true, 2),   // b was just deleted once
        Row("c", null, true, 3),   // c was deleted and then updated twice
        Row("c", "20", false, 4),
        Row("c", "200", false, 5),
        Row("e", "100", false, 6)  // new key
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("newValue", StringType),
          StructField("deleted", BooleanType), StructField("time", IntegerType))))
    cdc.createOrReplaceTempView("changes")

    cdc = sql("SELECT key, latest.newValue as newValue, latest.deleted as deleted FROM ( SELECT " +
              "key, max(struct(time, newValue, deleted)) as latest FROM changes GROUP BY key)")

    val updateMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    val insertMap = Map("key" -> "B.key", "value" -> "B.newValue").asInstanceOf[Map[Any, Any]]

    target.as("A").merge(cdc.as("B"), "A.key=B.key").
      whenMatched("B.deleted=false").
      updateExpr(updateMap).
      whenNotMatched("B.deleted=false").
      insertExpr(insertMap).
      whenMatched("B.deleted=true").
      delete().execute()
    assert(getDeleteDeltaFileCount("target", "0") == 1)
    checkAnswer(sql("select count(*) from target"), Seq(Row(3)))
    checkAnswer(sql("select * from target order by key"),
      Seq(Row("c", "200"), Row("d", "3"), Row("e", "100")))
  }

  test("check the cdc delete with partition") {
    sql("drop table if exists target")

    val initframe = sqlContext.sparkSession.createDataFrame(Seq(
      Row("a", "0"),
      Row("a1", "0"),
      Row("b", "1"),
      Row("c", "2"),
      Row("d", "3")
    ).asJava, StructType(Seq(StructField("key", StringType), StructField("value", StringType))))

    initframe.write
      .format("carbondata")
      .option("tableName", "target")
      .option("partitionColumns", "value")
      .mode(SaveMode.Overwrite)
      .save()
    val target = sqlContext.read.format("carbondata").option("tableName", "target").load()
    var cdc =
      sqlContext.sparkSession.createDataFrame(Seq(
        Row("a", null, true, 1),
        Row("a1", null, false, 1),
        Row("b", null, true, 2),
        Row("c", null, true, 3),
        Row("e", "100", false, 6)
      ).asJava,
        StructType(Seq(StructField("key", StringType),
          StructField("newValue", StringType),
          StructField("deleted", BooleanType), StructField("time", IntegerType))))
    cdc.createOrReplaceTempView("changes")

    cdc = sql("SELECT key, latest.newValue as newValue, latest.deleted as deleted FROM ( SELECT " +
              "key, max(struct(time, newValue, deleted)) as latest FROM changes GROUP BY key)")

    target.as("A").merge(cdc.as("B"), "A.key=B.key").
      whenMatched("B.deleted=true").delete().execute()

    assert(getDeleteDeltaFileCount("target", "0") == 1)
    checkAnswer(sql("select count(*) from target"), Seq(Row(2)))
    checkAnswer(sql("select * from target order by key"), Seq(Row("a1", "0"), Row("d", "3")))
  }

  test("test merge with table level date and timestamp format") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initializeWithDateTimeFormat
    val insertMap = Map("id" -> "B.id",
      "name" -> "B.name",
      "c_name" -> "B.c_name",
      "quantity" -> "B.quantity",
      "price" -> "B.price",
      "state" -> "B.state",
      "date" -> "B.date",
      "time" -> "B.time").asInstanceOf[Map[Any, Any]]
    dwSelframe.merge(odsframe, col("A.id").equalTo(col("B.id"))).whenMatched().
      insertExpr(insertMap).execute()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    // in case of cdc, the insert into flow goes to no converter step to save time as incoming data
    // from source will be correct, we wont use the table level timestamp format or load level for
    // the insert into of cdc data.
    checkAnswer(
      sql("select date,time from order where id = 'id1'"),
      Seq(
        Row(new Date(sdf.parse("2015-07-23").getTime), Timestamp.valueOf("2015-03-03 12:25:00")),
        Row(new Date(sdf.parse("2015-07-23").getTime), Timestamp.valueOf("2015-05-23 10:30:30"))
      ))
    checkAnswer(
      sql("select date,time from order where id = 'id11'"),
      Seq(
        Row(new Date(sdf.parse("2020-07-01").getTime), Timestamp.valueOf("2020-04-04 09:40:05.205"))
      ))
  }

  test("test merge update and insert with condition and expression " +
       "and delete action with target table as bucketing") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initializeWithBucketing(Seq("id"))

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state"))

    matches ++= Seq(WhenMatched(Some(col("A.state") =!= col("B.state")))
      .addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatched().addAction(InsertAction(insertMap)))
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget().addAction(DeleteAction()))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches(col("A.id").equalTo(col("B.id")), matches.toList))
      .run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 2)
    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
  }

  test("test merge with target table as multiple bucketing columns and join columns") {
    sql("drop table if exists order")
    val (dwSelframe, odsframe) = initializeWithBucketing(Seq("id", "quantity"))

    var matches = Seq.empty[MergeMatch]
    val updateMap = Map(col("id") -> col("A.id"),
      col("price") -> expr("B.price + 1"),
      col("state") -> col("B.state"))

    val insertMap = Map(col("id") -> col("B.id"),
      col("name") -> col("B.name"),
      col("c_name") -> col("B.c_name"),
      col("quantity") -> col("B.quantity"),
      col("price") -> expr("B.price * 100"),
      col("state") -> col("B.state"))

    matches ++= Seq(
      WhenMatched(Some(col("A.state") =!= col("B.state"))).addAction(UpdateAction(updateMap)))
    matches ++= Seq(WhenNotMatched().addAction(InsertAction(insertMap)))
    matches ++= Seq(WhenNotMatchedAndExistsOnlyOnTarget().addAction(DeleteAction()))

    CarbonMergeDataSetCommand(dwSelframe,
      odsframe,
      MergeDataSetMatches((col("A.id").equalTo(col("B.id"))).and(col("A.quantity").equalTo(col(
        "B.quantity"))), matches.toList)).run(sqlContext.sparkSession)
    assert(getDeleteDeltaFileCount("order", "0") == 1)
    checkAnswer(sql("select count(*) from order where id like 'newid%'"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from order"), Seq(Row(10)))
    checkAnswer(sql("select count(*) from order where state = 2"), Seq(Row(2)))
    checkAnswer(sql("select price from order where id = 'newid1'"), Seq(Row(7500)))
  }

  case class Target (id: Int, value: String, remark: String, mdt: String)
  case class Change (id: Int, value: String, change_type: String, mdt: String)
  private val numInitialRows = 10
  private val numInsertPerBatch = 2
  private val numUpdatePerBatch = 5
  private val numDeletePerBatch = 0
  private val numBatch = 30
  private val random = new Random()
  private val values =
    (1 to 100).map { x =>
      random.nextString(10)
    }
  private def pickValue = values(random.nextInt(values.size))
  private val currentIds = new java.util.ArrayList[Int](numInitialRows * 2)
  private def getId(index: Int) = currentIds.get(index)
  private def getAndRemoveId(index: Int) = currentIds.remove(index)
  private def addId(id: Int) = currentIds.add(id)
  private def removeId(index: Int) = currentIds.remove(index)
  private def numOfIds = currentIds.size
  private def maxId: Int = currentIds.asScala.max
  private val INSERT = "I"
  private val UPDATE = "U"
  private val DELETE = "D"

  private def generateRowsForInsert(sparkSession: SparkSession) = {
    // data for insert to the target table
    val insertRows = (maxId + 1 to maxId + numInsertPerBatch).map { x =>
      addId(x)
      Change(x, pickValue, INSERT, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(insertRows)
  }

  private def generateRowsForDelete(sparkSession: SparkSession) = {
    val deletedRows = (1 to numDeletePerBatch).map { x =>
      val idIndex = random.nextInt(numOfIds)
      Change(getAndRemoveId(idIndex), "", DELETE, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(deletedRows)
  }

  private def generateRowsForUpdate(sparkSession: SparkSession) = {
    val updatedRows = (1 to numUpdatePerBatch).map { x =>
      val idIndex = random.nextInt(numOfIds)
      Change(getId(idIndex), pickValue, UPDATE, LocalDateTime.now().toString)
    }
    sparkSession.createDataFrame(updatedRows)
  }

  // generate initial data for target table
  private def generateTarget(sparkSession: SparkSession): Unit = {
    val time = timeIt { () =>
      val insertRows = (1 to numInitialRows).map { x =>
        addId(x)
        Target(x, pickValue, "origin", LocalDateTime.now().toString)
      }
      val targetData = sparkSession.createDataFrame(insertRows)
      targetData.repartition(8)
        .write
        .format("carbondata")
        .option("tableName", "target")
        .option("sort_scope", "global_sort")
        .option("sort_column", "id")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  // generate change data
  private def generateChange(sparkSession: SparkSession): Unit = {
    val update = generateRowsForUpdate(sparkSession)
    val delete = generateRowsForDelete(sparkSession)
    val insert = generateRowsForInsert(sparkSession)
    // union them so that the change contains IUD
    update
      .union(delete)
      .union(insert)
      .repartition(8)
      .write
      .format("carbondata")
      .option("tableName", "change")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def readTargetData(sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read
      .format("carbondata")
      .option("tableName", "target")
      .load()

  private def readChangeData(sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read
      .format("carbondata")
      .option("tableName", "change")
      .load()

  private def timeIt(func: () => Unit): Long = {
    val start = System.nanoTime()
    func()
    System.nanoTime() - start
  }

  test("test cdc with compaction") {
    CarbonProperties.getInstance().addProperty("carbon.enable.auto.load.merge", "true")
    sql("drop table if exists target")
    sql("drop table if exists change")
    // prepare target table
    generateTarget(sqlContext.sparkSession)
    // Do CDC for N batch
    (1 to numBatch).foreach { i =>
      // prepare for change data
      generateChange(sqlContext.sparkSession)
      // apply change to target table
      import org.apache.spark.sql.CarbonSession._

      // find the latest value for each key
      val latestChangeForEachKey = readChangeData(sqlContext.sparkSession)
        .selectExpr("id", "struct(mdt, value, change_type) as otherCols")
        .groupBy("id")
        .agg(max("otherCols").as("latest"))
        .selectExpr("id", "latest.*")

      val target = readTargetData(sqlContext.sparkSession)
      target.as("A")
        .merge(latestChangeForEachKey.as("B"), "A.id = B.id")
        .whenMatched("B.change_type = 'D'")
        .delete()
        .whenMatched("B.change_type = 'U'")
        .updateExpr(
          Map("id" -> "B.id", "value" -> "B.value", "remark" -> "'updated'", "mdt" -> "B.mdt"))
        .whenNotMatched("B.change_type = 'I'")
        .insertExpr(
          Map("id" -> "B.id", "value" -> "B.value", "remark" -> "'new'", "mdt" -> "B.mdt"))
        .execute()
    }

    sql("clean files for table target").collect()
    checkAnswer(sql("select count(*) from target"), Seq(Row(70)))

    CarbonProperties.getInstance().addProperty("carbon.enable.auto.load.merge", "false")
  }

  private def getDeleteDeltaFileCount(tableName: String, segment: String): Int = {
    val table = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    var path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    if (table.isHivePartitionTable) {
      path = table.getAbsoluteTableIdentifier.getTablePath
    }
    val deleteDeltaFiles = FileFactory.getCarbonFile(path).listFiles(true, new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(CarbonCommonConstants
        .DELETE_DELTA_FILE_EXT)
    })
    deleteDeltaFiles.size()
  }

  override def afterAll {
    sql("drop table if exists order")
    sql("drop table if exists target")
    sql("drop table if exists change")
  }
}
