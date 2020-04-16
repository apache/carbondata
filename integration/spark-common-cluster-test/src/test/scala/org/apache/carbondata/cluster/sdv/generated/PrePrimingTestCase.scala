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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util_
import org.apache.spark.catalyst.analysis.NoSuchTableException

import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.carbondata.spark.exception.ProcessMetadataException

import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for PrePrimingTestCase to verify all scenarios
 * */

class PrePrimingTestCase extends QueryTest with BeforeAndAfterAll {

  // check prepriming with load
  test("Test_Load", Include) {
    sql(s"""drop table if exists test1""").collect
    sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
    Thread.sleep(5000)
    val preLoad = sql(s"""show metacache""").collect
    sql(s"""Insert into test1 select 'xx',1""").collect
    Thread.sleep(5000)
    val postLoad = sql(s"""show metacache""").collect
    assert(preLoad != postLoad)
    sql(s"""drop table if exists test1""").collect
  }

  //checking prepriming with compaction
  test("Test_Load", Include) {
    sql(s"""drop table if exists test1""").collect
    sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
    sql(s"""Insert into test1 select 'xx',1""").collect
    sql(s"""Insert into test1 select 'xx',1""").collect
    Thread.sleep(5000)
    val preCompaction = sql(s"""show metacache""").collect
    sql(s"""alter table test1 compact 'custom' where segment.id in (0,1)""")
    Thread.sleep(5000)
    val postCompaction = sql(s"""show metacache""").collect
    assert(preCompaction != postLoad)
    sql(s"""drop table if exists test1""").collect
  }

  val prop = CarbonProperties.getInstance()

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.indexserver.enable.prepriming", "true")
    prop.addProperty("carbon.enable.index.server", "true")
    prop.addProperty("carbon.index.server.ip", "")
    prop.addProperty("carbon.index.server.port", "")
    prop.addProperty("carbon.disable.index.server.fallback", "true")
    prop.addProperty("carbon.max.executor.lru.cache.size", "-1")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.enable.index.server", "false")
    sql("drop table if exists test1")
  }

}
