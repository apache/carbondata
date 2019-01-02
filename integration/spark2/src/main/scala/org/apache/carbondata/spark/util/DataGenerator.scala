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

package org.apache.carbondata.spark.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object DataGenerator {
  // Table schema:
  // +-------------+-----------+-------------+-------------+------------+
  // | Column name | Data type | Cardinality | Column type | Dictionary |
  // +-------------+-----------+-------------+-------------+------------+
  // | id          | string    | 100,000,000 | dimension   | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | city        | string    | 6           | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | country     | string    | 6           | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | planet      | string    | 100,000     | dimension   | yes        |
  // +-------------+-----------+-------------+-------------+------------+
  // | m1          | short     | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m2          | int       | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m3          | big int   | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m4          | double    | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  // | m5          | decimal   | NA          | measure     | no         |
  // +-------------+-----------+-------------+-------------+------------+
  /**
   * generate DataFrame with above table schema
   *
   * @param spark SparkSession
   * @return Dataframe of test data
   */
  def generateDataFrame(spark: SparkSession, totalNum: Int): DataFrame = {
    val rdd = spark.sparkContext
      .parallelize(1 to totalNum, 4)
      .map { x =>
        ((x % 100000000).toString, "city" + x % 6, "country" + x % 6, "planet" + x % 100000,
          (x % 16).toShort, x / 2, (x << 1).toLong, x.toDouble / 13,
          BigDecimal.valueOf(x.toDouble / 11))
      }.map { x =>
      Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)
    }

    val schema = StructType(
      Seq(
        StructField("id", StringType, nullable = false),
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("planet", StringType, nullable = false),
        StructField("m1", ShortType, nullable = false),
        StructField("m2", IntegerType, nullable = false),
        StructField("m3", LongType, nullable = false),
        StructField("m4", DoubleType, nullable = false),
        StructField("m5", DecimalType(30, 10), nullable = false)
      )
    )

    val df = spark.createDataFrame(rdd, schema)

    // scalastyle:off println
    println(s"Start generate ${df.count} records, schema: ${df.schema}")
    // scalastyle:on println

    df
  }
}
