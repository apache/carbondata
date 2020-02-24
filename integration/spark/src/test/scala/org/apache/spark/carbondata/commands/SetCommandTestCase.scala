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
package org.apache.spark.carbondata.commands

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.hive.execution.command.CarbonSetCommand
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}

class SetCommandTestCase extends QueryTest with BeforeAndAfterAll{
  override def beforeAll: Unit = {
    sql("set carbon=true")
  }
  test("test set command") {
    checkAnswer(sql("set"), sql("set"))
  }

  test("test set any value command") {
    checkAnswer(sql("set carbon=false"), sql("set carbon"))
  }

  test("test set command for enable.unsafe.sort=true") {
    checkAnswer(sql("set enable.unsafe.sort=true"), sql("set enable.unsafe.sort"))
  }

  test("test set command for enable.unsafe.sort for invalid option") {
    intercept[InvalidConfigurationException] {
      checkAnswer(sql("set enable.unsafe.sort=123"), sql("set enable.unsafe.sort"))
    }
  }
  //is_empty_data_bad_record
  test(s"test set command for" +
       s" ${ CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE }=true") {
    checkAnswer(sql(s"set ${
      CarbonLoadOptionConstants
        .CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE
    }=true"), sql(s"set ${ CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE }"))
  }

  test(s"test set command for ${
    CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE} for invalid option") {
    intercept[InvalidConfigurationException] {
      checkAnswer(sql(s"set ${
        CarbonLoadOptionConstants
          .CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE
      }=123"), sql(s"set ${ CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE }"))
    }
  }
  test(s"test set command for ${
    CarbonLoadOptionConstants
      .CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD
  }=true") {
    checkAnswer(sql(s"set ${
      CarbonLoadOptionConstants
        .CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD
    }=true"),
      sql(s"set ${ CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD }"))
  }

  test(s"test set command for ${CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD} " +
       s"for invalid option") {
    intercept[InvalidConfigurationException] {
      checkAnswer(
        sql(s"set ${CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD}=123"),
        sql(s"set ${CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD}"))
    }
  }
  //carbon.custom.block.distribution
  test("test set command for carbon.custom.block.distribution=true") {
    checkAnswer(sql("set carbon.custom.block.distribution=true"),
      sql("set carbon.custom.block.distribution"))
  }

  test("test set command for carbon.custom.block.distribution for invalid option") {
    intercept[InvalidConfigurationException] {
      checkAnswer(sql("set carbon.custom.block.distribution=123"),
        sql("set carbon.custom.block.distribution"))
    }
  }
  // sort_scope
  test(s"test set command for ${CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE}=LOCAL_SORT") {
    checkAnswer(sql(s"set ${CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE}=LOCAL_SORT"),
      sql(s"set ${CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE}"))
  }

  test(s"test set command for ${CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE} for invalid option") {
    intercept[InvalidConfigurationException] {
      checkAnswer(sql(s"set ${CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE}=123"),
        sql(s"set ${CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE}"))
    }
  }

  test(s"test set carbon.table.load.sort.scope for valid options") {
    checkAnswer(
      sql(s"set carbon.table.load.sort.scope.db.tbl=no_sort"),
      sql(s"set carbon.table.load.sort.scope.db.tbl"))

    checkAnswer(
      sql(s"set carbon.table.load.sort.scope.db.tbl=local_sort"),
      sql(s"set carbon.table.load.sort.scope.db.tbl"))

    checkAnswer(
      sql(s"set carbon.table.load.sort.scope.db.tbl=global_sort"),
      sql(s"set carbon.table.load.sort.scope.db.tbl"))
  }

  test(s"test set carbon.table.load.sort.scope for invalid options")
  {
    intercept[InvalidConfigurationException] {
      checkAnswer(
        sql(s"set carbon.table.load.sort.scope.db.tbl=fake_sort"),
        sql(s"set carbon.table.load.sort.scope.db.tbl"))
    }
  }

  override def afterAll {
    defaultConfig()
    sqlContext.sparkSession.catalog.clearCache()
    Seq("carbon", "carbon.table.load.sort.scope.db.tbl").foreach { key =>
      sqlContext.sparkSession.conf.unset(key)
    }
  }
}
