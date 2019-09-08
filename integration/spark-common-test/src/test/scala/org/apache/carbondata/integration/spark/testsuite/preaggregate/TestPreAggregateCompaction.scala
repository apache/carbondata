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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.Matchers._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestPreAggregateCompaction extends CarbonQueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeEach(): Unit = {
    sql("drop database if exists compaction cascade")
    sql("create database if not exists compaction")
    sql("use compaction")
    sql("create table testtable (id int, name string, city string, age int) STORED BY 'org.apache.carbondata.format'")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id,sum(age) from maintable group by id"""
        .stripMargin)
    sql(
      s"""create datamap preagg_avg on table maintable using 'preaggregate' as select id,avg(age) from maintable group by id"""
        .stripMargin)
  }

  test("test if pre-agg table is compacted with parent table minor compaction") {
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("insert into testtable select * from maintable")
    val sumResult = sql("select id, sum(age) from testtable group by id").collect()
    val avgResult = sql("select id, sum(age), count(age) from testtable group by id").collect()
    sql("alter table maintable compact 'minor'")
    val segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("3", "2", "1", "0.1", "0"))
    checkAnswer(sql("select * from maintable_preagg_sum"), sumResult)
    val segmentNamesAvg = sql("show segments for table maintable_preagg_avg").collect().map(_.get(0).toString)
    segmentNamesAvg should equal (Array("3", "2", "1", "0.1", "0"))
    checkAnswer(sql("select * from maintable_preagg_avg"), avgResult)
  }

  test("test if pre-agg table is compacted with parent table major compaction") {
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable compact 'major'")
    sql("insert into testtable select * from maintable")
    val sumResult = sql("select id, sum(age) from testtable group by id").collect()
    val avgResult = sql("select id, sum(age), count(age) from testtable group by id").collect()
    sql("alter table maintable compact 'minor'")
    val segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("3", "2", "1", "0.1", "0"))
    checkAnswer(sql("select * from maintable_preagg_sum"), sumResult)
    val segmentNamesAvg = sql("show segments for table maintable_preagg_avg").collect().map(_.get(0).toString)
    segmentNamesAvg should equal (Array("3", "2", "1", "0.1", "0"))
    checkAnswer(sql("select * from maintable_preagg_avg"), avgResult)
  }

  test("test if 2nd level minor compaction is successful for pre-agg table") {
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable compact 'minor'")
    var segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("3", "2", "1", "0.1", "0"))
    sql("insert into testtable select * from maintable")
    var sumResult = sql("select id, sum(age) from testtable group by id").collect()
    var avgResult = sql("select id, sum(age), count(age) from testtable group by id").collect()
    checkAnswer(sql("select * from maintable_preagg_sum"), sumResult)
    var segmentNamesAvg = sql("show segments for table maintable_preagg_avg").collect().map(_.get(0).toString)
    segmentNamesAvg should equal (Array("3", "2", "1", "0.1", "0"))
    checkAnswer(sql("select * from maintable_preagg_avg"), avgResult)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable compact 'minor'")
    sql("insert overwrite table testtable select * from maintable")
    sumResult = sql("select id, sum(age) from testtable group by id").collect()
    avgResult = sql("select id, sum(age), count(age) from testtable group by id").collect()
    segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum.sorted should equal (Array("0", "0.1", "1", "2", "3", "4", "4.1", "5", "6", "7"))
    checkAnswer(sql("select maintable_id, sum(maintable_age_sum) from maintable_preagg_sum group by maintable_id"), sumResult)
    segmentNamesAvg = sql("show segments for table maintable_preagg_avg").collect().map(_.get(0).toString)
    segmentNamesAvg.sorted should equal (Array("0", "0.1", "1", "2", "3", "4", "4.1", "5", "6", "7"))
    checkAnswer(sql("select maintable_id, sum(maintable_age_sum), sum(maintable_age_count) from maintable_preagg_avg group by maintable_id"), avgResult)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable compact 'minor'")
    sql("insert overwrite table testtable select * from maintable")
    sumResult = sql("select id, sum(age) from testtable group by id").collect()
    avgResult = sql("select id, sum(age), count(age) from testtable group by id").collect()
    segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("11", "10", "9", "8.1", "8", "7", "6", "5", "4.1", "4", "3", "2", "1", "0.2", "0.1", "0"))
    checkAnswer(sql("select maintable_id, sum(maintable_age_sum) from maintable_preagg_sum group by maintable_id"), sumResult)
    segmentNamesAvg = sql("show segments for table maintable_preagg_avg").collect().map(_.get(0).toString)
    segmentNamesAvg should equal (Array("11", "10", "9", "8.1", "8", "7", "6", "5", "4.1", "4", "3", "2", "1", "0.2", "0.1", "0"))
    checkAnswer(sql("select maintable_id, sum(maintable_age_sum), sum(maintable_age_count) from maintable_preagg_avg group by maintable_id"), avgResult)
  }

  test("test direct minor compaction on pre-agg tables") {
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable_preagg_sum compact 'minor'")
    var segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("3", "2", "1", "0.1", "0"))
    sql("insert into testtable select * from maintable")
    var sumResult = sql("select id, sum(age) from testtable group by id").collect()
    checkAnswer(sql("select * from maintable_preagg_sum"), sumResult)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable_preagg_sum compact 'minor'")
    sql("insert overwrite table testtable select * from maintable")
    sumResult = sql("select id, sum(age) from testtable group by id").collect()
    segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum.sorted should equal (Array("0", "0.1", "1", "2", "3", "4", "4.1", "5", "6", "7"))
    checkAnswer(sql("select maintable_id, sum(maintable_age_sum) from maintable_preagg_sum group by maintable_id"), sumResult)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable_preagg_sum compact 'minor'")
    sql("insert overwrite table testtable select * from maintable")
    sumResult = sql("select id, sum(age) from testtable group by id").collect()
    segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("11", "10", "9", "8.1", "8", "7", "6", "5", "4.1", "4", "3", "2", "1", "0.2", "0.1", "0"))
    checkAnswer(sql("select maintable_id, sum(maintable_age_sum) from maintable_preagg_sum group by maintable_id"), sumResult)
    val mainTableSegment = sql("SHOW SEGMENTS FOR TABLE maintable")
    val SegmentSequenceIds = mainTableSegment.collect().map { each => (each.toSeq) (0) }
    assert(!SegmentSequenceIds.contains("0.1"))
  }

  test("test if minor/major compaction is successful for pre-agg table") {
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable_preagg_sum compact 'minor'")
    var segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum should equal (Array("3","2","1","0.1", "0"))
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable_preagg_sum compact 'major'")
    segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum.sorted should equal (Array("0", "0.1", "0.2", "1", "2", "3", "4", "5", "6", "7"))
  }

  test("test auto compaction on aggregate table") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    val segmentNamesSum = sql("show segments for table maintable_preagg_sum").collect().map(_.get(0).toString)
    segmentNamesSum.sorted should equal  (Array("0", "0.1", "1", "2", "3"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
  }

  test("test minor compaction on Pre-agg tables after multiple loads") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    sql("alter table maintable compact 'minor'")
    assert(sql("show segments for table maintable").collect().map(_.get(1).toString.toLowerCase).contains("compacted"))
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    sql("drop database if exists compaction cascade")
    sql("use default")
  }

}
