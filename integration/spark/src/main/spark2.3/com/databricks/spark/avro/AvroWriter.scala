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
package com.databricks.spark.avro

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriterFactory

/**
 * This class is to get the avro writer from databricks avro module, as its not present in spark2.3
 * and spark-avro module is included in spark project from spark-2.4. So for spark-2.4, we use Avro
 * writer from spark project.
 */
object AvroWriter {

  def getWriter(spark: org.apache.spark.sql.SparkSession,
      job: org.apache.hadoop.mapreduce.Job,
      dataSchema: org.apache.spark.sql.types.StructType,
      options: scala.Predef.Map[scala.Predef.String, scala.Predef.String] = Map.empty)
  : OutputWriterFactory = {
    new DefaultSource().prepareWrite(spark, job,
      options, dataSchema)
  }
}

/**
 * This reads the avro files from the given path and return the RDD[Row]
 */
object AvroReader {

  def readAvro(spark: org.apache.spark.sql.SparkSession, deltaPath: String): RDD[Row] = {
    spark.sparkContext
      .hadoopConfiguration
      .set("avro.mapred.ignore.inputs.without.extension", "false")
    spark.read.avro(deltaPath).rdd
  }
}
