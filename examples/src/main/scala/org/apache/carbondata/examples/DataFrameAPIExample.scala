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

import org.apache.spark.sql.SaveMode

import org.apache.carbondata.examples.util.InitForExamples

// scalastyle:off println
object DataFrameAPIExample {

  def main(args: Array[String]) {
    val cc = InitForExamples.createCarbonContext("DataFrameAPIExample")
    val sc = cc.sc

    import cc.implicits._

    // create a dataframe, it can be from parquet or hive table
    val df = sc.parallelize(1 to 1000)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "c3")

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .mode(SaveMode.Overwrite)
      .save()

    // use datasource api to read
    val in = cc.read
      .format("carbondata")
      .option("tableName", "carbon1")
      .load()

    val count = in.where($"c3" > 500).select($"*").count()
    println(s"count using dataframe.read: $count")

    // use SQL to read
    cc.sql("SELECT count(*) FROM carbon1 WHERE c3 > 500").show
    cc.sql("DROP TABLE IF EXISTS carbon1")

    // also support a implicit function for easier access
    import org.apache.carbondata.spark._
    df.saveAsCarbonFile(Map("tableName" -> "carbon2", "compress" -> "true"))

    cc.sql("SELECT count(*) FROM carbon2 WHERE c3 > 100").show
    cc.sql("DROP TABLE IF EXISTS carbon2")
  }
}
// scalastyle:on println
