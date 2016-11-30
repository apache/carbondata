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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.spark.sql.SQLContext

/**
 * This example needs Spark 1.6 or later version to run
 */
object DirectSQLExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("DatasourceExample")
    ExampleUtils.writeSampleCarbonFile(cc, "table1")

    // Use SQLContext to read CarbonData files without creating table
    val sqlContext = new SQLContext(cc.sparkContext)
    sqlContext.sql(
      s"""
        | SELECT c1, c2, count(*)
        | FROM carbondata.`${cc.storePath}/${CarbonCommonConstants.DATABASE_DEFAULT_NAME}/table1`
        | WHERE c3 > 100
        | GROUP BY c1, c2
      """.stripMargin).show
  }
}
