/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the"License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an"AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.testsuite.datacompaction

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}


class CompactionSupportSpecifiedSegmentsTest
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  val filePath: String = resourcesPath + "/globalsort/sample1.csv"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def beforeEach {
    resetConf()
    sql("DROP TABLE IF EXISTS seg_compact")
    sql(
      """
        |CREATE TABLE seg_compact
        |(id INT, name STRING, city STRING, age INT)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS'='city,name')
      """.stripMargin)
  }

  override def afterEach {
    sql("DROP TABLE IF EXISTS seg_compact")
  }

  private def resetConf() = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  }

  test("custom compaction") {
    for (i <- 0 until 5) {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE seg_compact")
    }
    sql("ALTER TABLE seg_compact COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2,3)")

    val segments = sql("SHOW SEGMENTS FOR TABLE seg_compact")
    val segInfos = segments.collect().map { each =>
      ((each.toSeq) (0).toString, (each.toSeq) (1).toString)
    }
    assert(segInfos.length == 6)
    assert(segInfos.contains(("0", "Success")))
    assert(segInfos.contains(("1", "Compacted")))
    assert(segInfos.contains(("2", "Compacted")))
    assert(segInfos.contains(("3", "Compacted")))
    assert(segInfos.contains(("1.1", "Success")))
    assert(segInfos.contains(("4", "Success")))
  }

  test("custom compaction with invalid segment id"){
    try{
      for (i <- 0 until 5) {
        sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE seg_compact")
      }
      sql("DELETE FROM TABLE seg_compact WHERE SEGMENT.ID IN (1)")
      sql("ALTER TABLE seg_compact COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (1,2,3)")
    }catch {
      case e: Exception =>
        assert(e.getMessage.contains("does not exist or is not valid"))
    }
  }

  test("custom segment ids must not be empty in custom compaction"){
    try{
      for (i <- 0 until 5) {
        sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE seg_compact")
      }
      sql("ALTER TABLE seg_compact COMPACT 'CUSTOM'")
    }catch {
      case e: Exception =>
        assert(e.isInstanceOf[MalformedCarbonCommandException])
        assert(e.getMessage.startsWith("Segment ids should not be empty"))
    }
  }

  test("custom segment ids not supported in major compaction"){
    try{
      for (i <- 0 until 5) {
        sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE seg_compact")
      }
      sql("ALTER TABLE seg_compact COMPACT 'MAJOR' WHERE SEGMENT.ID IN (1,2,3)")
    }catch {
      case e: Exception =>
        assert(e.isInstanceOf[MalformedCarbonCommandException])
        assert(e.getMessage.startsWith("Custom segments not supported"))
    }
  }

  test("custom segment ids not supported in minor compaction"){
    try{
      for (i <- 0 until 5) {
        sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE seg_compact")
      }
      sql("ALTER TABLE seg_compact COMPACT 'MINOR' WHERE SEGMENT.ID IN (1,2,3)")
    }catch {
      case e: Exception =>
        assert(e.isInstanceOf[MalformedCarbonCommandException])
        assert(e.getMessage.startsWith("Custom segments not supported"))
    }
  }
}

