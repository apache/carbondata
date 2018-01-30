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

package org.apache.carbondata.store

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.store.api.{CarbonStore, SchemaBuilder}

class TestCarbonFileWriter extends QueryTest with BeforeAndAfterAll {
  val tablePath = "./db1/tc1"

  override def beforeAll(): Unit = {
    createTestTable(tablePath)
  }

  override def afterAll(): Unit = {
    cleanTestTable(tablePath)
  }

  test("test write carbon table and read as external table") {
    sql(s"CREATE EXTERNAL TABLE source STORED BY 'carbondata' LOCATION '$tablePath'")
    checkAnswer(sql("SELECT count(*) from source"), Row(1000))
    sql("DROP TABLE IF EXISTS source")
  }

  test("test write carbon table and read by refresh table") {
    sql("CREATE DATABASE db1 LOCATION './db1'")
    sql("REFRESH TABLE db1.tc1")
    checkAnswer(sql("SELECT count(*) from db1.tc1"), Row(1000))
    sql("DROP DATABASE IF EXISTS db1 CASCADE")
  }

  private def cleanTestTable(tablePath: String) = {
    if (new File(tablePath).exists()) {
      new File(tablePath).delete()
    }
  }

  private def createTestTable(tablePath: String): Unit = {
    val carbon = CarbonStore.build()

    val schema = SchemaBuilder.newInstance
      .addColumn(new StructField("name", DataTypes.STRING), true)
      .addColumn(new StructField("age", DataTypes.INT), false)
      .addColumn(new StructField("height", DataTypes.DOUBLE), false)
      .create

    try {
      val table = carbon.createTable("t1", schema, tablePath)
      val segment = table.newBatchSegment()

      segment.open()
      val writer = segment.newWriter()
      (1 to 1000).foreach { _ => writer.writeRow(Array[String]("amy", "1", "2.3")) }
      writer.close()
      segment.commit()
    } catch {
      case e:Throwable => fail(e.getMessage)
    }
  }

}
