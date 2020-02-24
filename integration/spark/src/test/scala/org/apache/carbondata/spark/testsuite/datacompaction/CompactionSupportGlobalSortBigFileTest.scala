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

package org.apache.carbondata.spark.testsuite.datacompaction

import java.io.{File, PrintWriter}

import scala.util.Random

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class CompactionSupportGlobalSortBigFileTest extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file1 = resourcesPath + "/compaction/fil1.csv"
  val file2 = resourcesPath + "/compaction/fil2.csv"
  val file3 = resourcesPath + "/compaction/fil3.csv"
  val file4 = resourcesPath + "/compaction/fil4.csv"
  val file5 = resourcesPath + "/compaction/fil5.csv"

  override protected def beforeAll(): Unit = {
    resetConf("10")
    //n should be about 5000000 of reset if size is default 1024
    val n = 150000
    CompactionSupportGlobalSortBigFileTest.createFile(file1, n, 0)
    CompactionSupportGlobalSortBigFileTest.createFile(file2, n * 4, n)
    CompactionSupportGlobalSortBigFileTest.createFile(file3, n * 3, n * 5)
    CompactionSupportGlobalSortBigFileTest.createFile(file4, n * 2, n * 8)
    CompactionSupportGlobalSortBigFileTest.createFile(file5, n * 2, n * 13)
  }

  override protected def afterAll(): Unit = {
    CompactionSupportGlobalSortBigFileTest.deleteFile(file1)
    CompactionSupportGlobalSortBigFileTest.deleteFile(file2)
    CompactionSupportGlobalSortBigFileTest.deleteFile(file3)
    CompactionSupportGlobalSortBigFileTest.deleteFile(file4)
    CompactionSupportGlobalSortBigFileTest.deleteFile(file5)
    resetConf(CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE)
  }

  override def beforeEach {
    sql("DROP TABLE IF EXISTS compaction_globalsort")
    sql(
      """
        | CREATE TABLE compaction_globalsort(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='global_sort')
      """.stripMargin)

    sql("DROP TABLE IF EXISTS carbon_localsort")
    sql(
      """
        | CREATE TABLE carbon_localsort(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
      """.stripMargin)
  }

  override def afterEach {
    sql("DROP TABLE IF EXISTS compaction_globalsort")
    sql("DROP TABLE IF EXISTS carbon_localsort")
  }

  test("Compaction major:  segments size is bigger than default compaction size") {
    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE carbon_localsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE carbon_localsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE carbon_localsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file4' INTO TABLE carbon_localsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file5' INTO TABLE carbon_localsort OPTIONS('header'='false')")

    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE compaction_globalsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE compaction_globalsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file3' INTO TABLE compaction_globalsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file4' INTO TABLE compaction_globalsort OPTIONS('header'='false')")
    sql(s"LOAD DATA LOCAL INPATH '$file5' INTO TABLE compaction_globalsort OPTIONS('header'='false')")

    sql("ALTER TABLE compaction_globalsort COMPACT 'MAJOR'")
    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "global_sort")

    checkExistence(sql("DESCRIBE FORMATTED compaction_globalsort"), true, "city,name")

    checkExistence(sql("SHOW SEGMENTS FOR TABLE compaction_globalsort"), true, "Compacted")

    checkAnswer(sql("select count(*) from compaction_globalsort"),sql("select count(*) from carbon_localsort"))
    val segments = sql("SHOW SEGMENTS FOR TABLE compaction_globalsort")
    val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
    assert(SegmentSequenceIds.contains("0.1"))
  }

  private def resetConf(size:String) {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE, size)
  }
}

object CompactionSupportGlobalSortBigFileTest {
  def createFile(fileName: String, line: Int = 10000, start: Int = 0): Boolean = {
    try {
      val write = new PrintWriter(fileName);
      for (i <- start until (start + line)) {
        write.println(i + "," + "n" + i + "," + "c" + (i % 10000) + "," + Random.nextInt(80))
      }
      write.close()
    } catch {
      case _: Exception => false
    }
    true
  }

  def deleteFile(fileName: String): Boolean = {
    try {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case _: Exception => false
    }
    true
  }
}
