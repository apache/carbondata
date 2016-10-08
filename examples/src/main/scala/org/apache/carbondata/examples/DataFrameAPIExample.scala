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

package org.apache.carbondata.examples

import org.apache.carbondata.examples.util.ExampleUtils

// scalastyle:off println
object DataFrameAPIExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("DataFrameAPIExample")
    ExampleUtils.writeSampleCarbonFile(cc, "carbon1")

    // use datasource api to read
    val in = cc.read
      .format("carbondata")
      .option("tableName", "carbon1")
      .load()

    import cc.implicits._
    val count = in.where($"c3" > 500).select($"*").count()
    println(s"count using dataframe: $count")

    // use SQL to read
    cc.sql("SELECT count(*) FROM carbon1 WHERE c3 > 500").show
    cc.sql("DROP TABLE IF EXISTS carbon1")
  }
}
// scalastyle:on println
