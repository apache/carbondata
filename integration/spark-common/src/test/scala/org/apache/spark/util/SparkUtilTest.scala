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

package org.apache.spark.util

import org.apache.spark.SPARK_VERSION
import org.scalatest.FunSuite

class SparkUtilTest extends FunSuite{

  test("Test Spark Version API with X and Above") {
    if (SPARK_VERSION.startsWith("2.1")) {
      assert(SparkUtil.isSparkVersionXandAbove("2.1"))
      assert(!SparkUtil.isSparkVersionXandAbove("2.2"))
      assert(!SparkUtil.isSparkVersionXandAbove("2.3"))
    } else if (SPARK_VERSION.startsWith("2.2")) {
      assert(SparkUtil.isSparkVersionXandAbove("2.1"))
      assert(SparkUtil.isSparkVersionXandAbove("2.2"))
      assert(!SparkUtil.isSparkVersionXandAbove("2.3"))
    } else {
      assert(SparkUtil.isSparkVersionXandAbove("2.1"))
      assert(SparkUtil.isSparkVersionXandAbove("2.2"))
      assert(SparkUtil.isSparkVersionXandAbove("2.3"))
      assert(!SparkUtil.isSparkVersionXandAbove("2.4"))
    }
  }

  test("Test Spark Version API Equal to X") {
    if (SPARK_VERSION.startsWith("2.1")) {
      assert(SparkUtil.isSparkVersionEqualToX("2.1"))
      assert(!SparkUtil.isSparkVersionEqualToX("2.2"))
      assert(!SparkUtil.isSparkVersionEqualToX("2.3"))
    } else if (SPARK_VERSION.startsWith("2.2")) {
      assert(!SparkUtil.isSparkVersionEqualToX("2.1"))
      assert(SparkUtil.isSparkVersionEqualToX("2.2"))
      assert(!SparkUtil.isSparkVersionEqualToX("2.3"))
    } else {
      assert(!SparkUtil.isSparkVersionEqualToX("2.1"))
      assert(!SparkUtil.isSparkVersionEqualToX("2.2"))
      assert(SparkUtil.isSparkVersionEqualToX("2.3"))
      assert(!SparkUtil.isSparkVersionEqualToX("2.4"))
    }
  }
}