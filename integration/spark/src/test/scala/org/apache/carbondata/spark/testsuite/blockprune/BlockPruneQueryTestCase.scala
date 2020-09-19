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
package org.apache.carbondata.spark.testsuite.blockprune

import java.io.DataOutputStream

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class contains test cases for block prune query
 */
class BlockPruneQueryTestCase extends QueryTest with BeforeAndAfterAll {
  val outputPath = s"$resourcesPath/block_prune_test.csv"
  val enableMultiThreadFilesCount = "1"
  val negativeMultiThreadFilesCount = "-1"
  val disableMultiThreadFilesCount =
    CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT_DEFAULT;

  def perpareCarbonProperty(propertyName: String,
      propertyValue: String): Unit = {
    val properties = CarbonProperties.getInstance()
    properties.addProperty(propertyName, propertyValue)
    assert(properties.getProperty(propertyName).equals(propertyValue))
  }

  def perpareData(): Unit = {
    sql(
      """CREATE TABLE IF NOT EXISTS blockprune (name string, id int)
        STORED AS carbondata""")
    sql(
      s"LOAD DATA LOCAL INPATH '$outputPath' INTO table blockprune options('FILEHEADER'='name,id')"
    )
  }


  override def beforeAll {
    // Since the data needed for block prune is big, need to create a temp data file
    val testData: Array[String] = new Array[String](3)
    testData(0) = "a"
    testData(1) = "b"
    testData(2) = "c"
    var writer: DataOutputStream = null
    try {
      val file = FileFactory.getCarbonFile(outputPath)
      if (!file.exists()) {
        file.createNewFile()
      }
      writer = FileFactory.getDataOutputStream(outputPath)
      for (i <- 0 to 2) {
        for (j <- 0 to 240000) {
          writer.writeBytes(testData(i) + "," + j + "\n")
        }
      }
    } catch {
      case ex: Exception =>
        LOGGER.error("Build test file for block prune failed", ex)
    } finally {
      if (writer != null) {
        try {
          writer.close()
        } catch {
          case ex: Exception =>
            LOGGER.error("Close output stream catching exception", ex)
        }
      }
    }
  }

  test("test block prune without filter") {
    sql("DROP TABLE IF EXISTS blockprune")
    perpareData()
    checkAnswer(
      sql(
        """select * from blockprune limit 1"""),
      Seq(Row("a", 0)))
  }

  test("test block prune with negative multiThreadFilesCount") {
    sql("DROP TABLE IF EXISTS blockprune")
    perpareCarbonProperty(
      CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT,
      negativeMultiThreadFilesCount)
    perpareData()
    checkAnswer(
      sql(
        """select * from blockprune limit 1"""),
      Seq(Row("a", 0)))
  }

  test("test block prune single thread") {
    sql("DROP TABLE IF EXISTS blockprune")
    perpareData()
    // data is in all 7 blocks
    checkAnswer(
      sql(
        """select name,count(name) as amount from blockprune
           where name='c' or name='b' or name='a' group by name"""),
      Seq(Row("a", 240001), Row("b", 240001), Row("c", 240001)))

    // data only in middle 3/4/5 blocks
    checkAnswer(
      sql(
        """select name,count(name) as amount from blockprune
          where name='b' group by name"""),
      Seq(Row("b", 240001)))
  }

  test("test block prune multi threads") {
    sql("DROP TABLE IF EXISTS blockprune")

    perpareCarbonProperty(
      CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT,
      enableMultiThreadFilesCount)

    sql(
      """
        CREATE TABLE IF NOT EXISTS blockprune (name string, id int)
        STORED AS carbondata
      """)

    val segmentCount = 10
    for (i <- 1 to segmentCount) {
      sql(s"LOAD DATA LOCAL INPATH '$outputPath' INTO table blockprune " +
          s"options('FILEHEADER'='name,id')")
    }
    // data is in all 7 blocks
    checkAnswer(
      sql(
        """
          select name,count(name) as amount from blockprune
          where name='c' or name='b' or name='a' group by name
        """),
      Seq(Row("a", 2400010), Row("b", 2400010), Row("c", 2400010)))

    // data only in middle 3/4/5 blocks
    checkAnswer(
      sql(
        """select name,count(name) as amount from blockprune
          where name='b' group by name"""),
      Seq(Row("b", 2400010)))

    perpareCarbonProperty(
      CarbonCommonConstants.CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT,
      disableMultiThreadFilesCount)
  }

  override def afterAll {
    // delete the temp data file
    try {
      val file = FileFactory.getCarbonFile(outputPath)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case ex: Exception =>
        LOGGER.error("Delete temp test data file for block prune catching exception", ex)
    } finally {
      sql("DROP TABLE IF EXISTS blockprune")
    }
  }

}
