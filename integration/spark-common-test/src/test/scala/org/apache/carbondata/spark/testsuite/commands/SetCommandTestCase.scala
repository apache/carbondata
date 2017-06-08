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
package org.apache.carbondata.spark.testsuite.commands

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.util.CarbonProperties

class SetCommandTestCase extends QueryTest with BeforeAndAfterAll {

  test("test set command") {
    checkAnswer(sql("set carbon=false"), sql("set carbon"))
  }

  test("test set command for enable.unsafe.sort=true") {
    checkAnswer(sql("set enable.unsafe.sort=false"), sql("set enable.unsafe.sort"))
  }

  test("test set command for enable.unsafe.sort for invalid option") {
    try {
      checkAnswer(sql("set enable.unsafe.sort=123"), sql("set enable.unsafe.sort"))
      assert(false)
    } catch {
      case ex: InvalidConfigurationException =>
        assert(true)
    }
  }
  //enable.unsafe.in.query.processing
  test("test set command for enable.offheap.sort=true") {
    checkAnswer(sql("set enable.offheap.sort=false"), sql("set enable.offheap.sort"))
  }

  test("test set command for enable.offheap.sort for invalid option") {
    try {
      checkAnswer(sql("set enable.offheap.sort=123"), sql("set enable.offheap.sort"))
      assert(false)
    } catch {
      case ex: InvalidConfigurationException =>
        assert(true)
    }
  }
  test("test set command for enable.unsafe.in.query.processing=true") {
    checkAnswer(sql("set enable.unsafe.in.query.processing=false"),
      sql("set enable.unsafe.in.query.processing"))
  }

  test("test set command for enable.unsafe.in.query.processing for invalid option") {
    try {
      checkAnswer(sql("set enable.unsafe.in.query.processing=123"),
        sql("set enable.unsafe.in.query.processing"))
      assert(false)
    } catch {
      case ex: InvalidConfigurationException =>
        assert(true)
    }
  }
  //carbon.custom.block.distribution
  test("test set command for carbon.custom.block.distribution=true") {
    checkAnswer(sql("set carbon.custom.block.distribution=false"),
      sql("set carbon.custom.block.distribution"))
  }

  test("test set command for carbon.custom.block.distribution for invalid option") {
    try {
      checkAnswer(sql("set carbon.custom.block.distribution=123"),
        sql("set carbon.custom.block.distribution"))
      assert(false)
    } catch {
      case ex: InvalidConfigurationException =>
        assert(true)
    }
  }
}
