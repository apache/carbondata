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

// scalastyle:off println
object DataFrameAPIExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("DataFrameAPIExample")
    ExampleUtils.writeSampleCarbonFile(spark, "carbon1", 1000)

    import spark.implicits._

    // use datasource api to read
    val in = spark.read
      .format("carbondata")
      .option("tableName", "carbon1")
      .load()
    var count = in.where($"c3" > 500).select($"*").count()
    println(s"count after 1 load: $count")

    // append new data, query answer should be 1000
    ExampleUtils.appendSampleCarbonFile(spark, "carbon1")
    count = in.where($"c3" > 500).select($"*").count()
    println(s"count after 2 load: $count")

    // use SQL to read
    spark.sql("SELECT c1, count(c3) FROM carbon1 where c3 > 500 group by c1 limit 10").show

    // delete carbondata file
    ExampleUtils.cleanSampleCarbonFile(spark, "carbon1")
  }
}
// scalastyle:on println
