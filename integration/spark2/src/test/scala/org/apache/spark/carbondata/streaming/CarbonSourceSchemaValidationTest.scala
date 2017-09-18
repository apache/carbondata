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

package org.apache.spark.carbondata.streaming

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.{CarbonSource, SparkSession}
import org.apache.spark.sql.streaming.CarbonStreamingOutputWriterFactory
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

/**
 * Test for schema validation during streaming ingestion
 * Validates streamed schema(source) against existing table(target) schema.
 */

class CarbonSourceSchemaValidationTest extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll() {
    sql("DROP TABLE IF EXISTS _carbon_stream_table_")
  }

  test("Testing validate schema method with correct values ") {

    val spark = SparkSession.builder
      .appName("StreamIngestSchemaValidation")
      .master("local")
      .getOrCreate()

    val carbonSource = new CarbonSource
    val job = new Job()
    val warehouseLocation = TestQueryExecutor.warehouse

    sql("CREATE TABLE _carbon_stream_table_(id int,name string)STORED BY 'carbondata'")
    val tablePath: String = s"$warehouseLocation/default/_carbon_stream_table_"
    val dataSchema = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true)))
    val res = carbonSource.prepareWrite(spark, job, Map("path" -> tablePath), dataSchema)
    assert(res.isInstanceOf[CarbonStreamingOutputWriterFactory])
  }

  override def afterAll() {
    sql("DROP TABLE IF EXISTS _carbon_stream_table_")
  }

}
