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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.common.util._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.scalatest.BeforeAndAfterAll

class BucketingTestCase extends QueryTest with BeforeAndAfterAll {

  var threshold: Int = _
  var timeformat = CarbonProperties.getInstance()
    .getProperty("carbon.timestamp.format", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    threshold = sqlContext.getConf("spark.sql.autoBroadcastJoinThreshold").toInt
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
    sql("DROP TABLE IF EXISTS bucket_table")
  }

  test("test exception if bucketcolumns be measure column") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS bucket_table")
      sql("CREATE TABLE bucket_table (ID Int, date Timestamp, country String, name String, phonetype String," +
          "serialname String, salary Int) STORED BY 'carbondata' TBLPROPERTIES " +
          "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='ID')")
    }
  }

  test("test exception if bucketcolumns be complex data type column") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS bucket_table")
      sql("CREATE TABLE bucket_table (Id int, number double, name string, " +
          "gamePoint array<double>, mac struct<num:double>) STORED BY 'carbondata' TBLPROPERTIES" +
          "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='gamePoint')")
    }
  }

  test("test multi columns as bucketcolumns") {
    sql("DROP TABLE IF EXISTS bucket_table")
    sql("CREATE TABLE bucket_table (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED BY 'carbondata' TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name,phonetype')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE bucket_table")
    val table: CarbonTable = CarbonMetadata.getInstance().getCarbonTable("default_bucket_table")
    if (table != null && table.getBucketingInfo != null) {
      assert(true)
    } else {
      assert(false, "Bucketing info does not exist")
    }
  }

  test("test multi columns as bucketcolumns with bucket join") {
    sql("DROP TABLE IF EXISTS bucket_table")
    sql("CREATE TABLE bucket_table (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED BY 'carbondata' TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='country,name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE bucket_table")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from bucket_table t1, bucket_table t2
        |where t1.country = t2.country and t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchangeExec => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket column join")
  }

  test("test non bucket column join") {
    sql("DROP TABLE IF EXISTS bucket_table")
    sql("CREATE TABLE bucket_table (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED BY 'carbondata' TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='country')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE bucket_table")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from bucket_table t1, bucket_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false

    plan.collect {
      case s: ShuffleExchangeExec => shuffleExists = true
    }
    assert(shuffleExists, "shuffle should exist on non-bucket column join")
  }

  test("test bucketcolumns through multi data loading plus compaction") {
    sql("DROP TABLE IF EXISTS bucket_table")
    sql("CREATE TABLE bucket_table (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED BY 'carbondata' TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    val numOfLoad = 10
    for (j <- 0 until numOfLoad) {
      sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE bucket_table")
    }
    sql("ALTER TABLE bucket_table COMPACT 'MAJOR'")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from bucket_table t1, bucket_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchangeExec => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  test("drop non-bucket column, test bucket column join") {
    sql("DROP TABLE IF EXISTS bucket_table")
    sql("CREATE TABLE bucket_table (ID Int, date Timestamp, country String, name String, phonetype String," +
        "serialname String, salary Int) STORED BY 'carbondata' TBLPROPERTIES " +
        "('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='name')")
    sql(s"LOAD DATA INPATH '$resourcesPath/source.csv' INTO TABLE bucket_table")

    sql("ALTER TABLE bucket_table DROP COLUMNS (ID,country)")

    val plan = sql(
      """
        |select t1.*, t2.*
        |from bucket_table t1, bucket_table t2
        |where t1.name = t2.name
      """.stripMargin).queryExecution.executedPlan
    var shuffleExists = false
    plan.collect {
      case s: ShuffleExchangeExec => shuffleExists = true
    }
    assert(!shuffleExists, "shuffle should not exist on bucket tables")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS bucket_table")
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", threshold.toString)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timeformat)
  }
}
