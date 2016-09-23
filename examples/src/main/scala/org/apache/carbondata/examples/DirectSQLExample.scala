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

import org.apache.spark.sql.{SaveMode, SQLContext}

import org.apache.carbondata.examples.util.InitForExamples

object DirectSQLExample {

  def main(args: Array[String]) {
    // use CarbonContext to write CarbonData files
    val cc = InitForExamples.createCarbonContext("DatasourceExample")
    import cc.implicits._
    val sc = cc.sparkContext
    // create a dataframe, it can be from parquet or hive table
    val df = sc.parallelize(1 to 1000)
        .map(x => ("a", "b", x))
        .toDF("c1", "c2", "c3")

    // save dataframe to CarbonData files
    df.write
        .format("carbondata")
        .option("tableName", "table1")
        .mode(SaveMode.Overwrite)
        .save()

    // Use SQLContext to read CarbonData files without creating table
    // This requires Spark 1.6 or later version to run
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      """
        | SELECT c1, c2, count(*)
        | FROM carbondata.`./examples/target/store/default/table1`
        | WHERE c3 > 100
        | GROUP BY c1, c2
      """.stripMargin).show
  }
}
