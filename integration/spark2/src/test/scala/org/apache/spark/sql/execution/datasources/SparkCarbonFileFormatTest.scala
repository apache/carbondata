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
package org.apache.spark.sql.execution.datasources

import java.io.File

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datastore.impl.FileFactory

class SparkCarbonFileFormatTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  val writerPath: File = new File(this.getClass.getResource("/").getPath
                                  +
                                  "../." +
                                  "./target/FileFormatStore")

  val dataFrame: DataFrame = sqlContext.read.option("header", "true")
    .csv(s"$resourcesPath/measureinsertintotest.csv")

  override def beforeEach(): Unit = {
    FileFactory.deleteAllFilesOfDir(writerPath)

  }

  test("test write using format") {
    dataFrame.write.format("carbonfile").save(writerPath.getAbsolutePath)
    val storeFiles = writerPath.listFiles()
    // length is supposed to be 4 as SUCCESS and SUCCESS.crc files are also written by spark.
    assert(storeFiles.length == 4)
    assert(storeFiles.exists(_.getName.contains(".carbondata")))
    assert(storeFiles.exists(_.getName.contains("index")))
  }

  test("test data by creating external table") {
    dataFrame.write.format("carbonfile").save(writerPath.getAbsolutePath)
    sql(
      s"""CREATE EXTERNAL TABLE formatTest(id int, name string, city string, age int) STORED BY
         |'carbondata' LOCATION
         |'$writerPath' """.stripMargin)
    val result = sql("select * from formatTest")
    assert(result.collect().length == 6)
  }

  test("test data by reading using carbonfile source") {
    dataFrame.write.format("carbonfile").save(writerPath.getAbsolutePath)
    val result = sqlContext.read.format("carbonfile").load(writerPath.getAbsolutePath)
    assert(result.collect().length == 6)
    checkAnswer(result.limit(1), Seq(Row("1", "david", "shenzhen", "31")))
  }

  override def afterAll():Unit = {
    sql("drop table if exists formatTest")
    FileFactory.deleteAllFilesOfDir(writerPath)
  }
}
