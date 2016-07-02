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
package com.databricks.spark.csv.newapi

import java.nio.charset.Charset

import com.databricks.spark.csv.util.TextFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.util.FileUtils

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.spark.rdd.CarbonDataRDDFactory

/**
 * create RDD use CarbonDataLoadInputFormat
 */
private[csv] object CarbonTextFile {

  private def newHadoopRDD(sc: SparkContext, location: String) = {
    val hadoopConfiguration = new Configuration(sc.hadoopConfiguration)
    hadoopConfiguration.setStrings(FileInputFormat.INPUT_DIR, location)
    hadoopConfiguration.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true)
    CarbonDataRDDFactory.configSplitMaxSize(sc, location, hadoopConfiguration)
    new NewHadoopRDD[LongWritable, Text](
      sc,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      hadoopConfiguration).setName("newHadoopRDD-spark-csv")
  }

  def withCharset(cc: SparkContext, location: String, charset: String): RDD[String] = {
    if (Charset.forName(charset) == TextFile.DEFAULT_CHARSET) {
      newHadoopRDD(cc, location).map(pair => pair._2.toString)
    } else {
      // can't pass a Charset object here cause its not serializable
      // TODO: maybe use mapPartitions instead?
      newHadoopRDD(cc, location).map(
        pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }
}
