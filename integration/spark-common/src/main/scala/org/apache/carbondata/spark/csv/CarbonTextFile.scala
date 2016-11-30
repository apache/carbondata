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
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * create RDD use CarbonDataLoadInputFormat
 */
object CarbonTextFile {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def configSplitMaxSize(context: SparkContext, filePaths: String,
      hadoopConfiguration: Configuration): Unit = {
    val defaultParallelism = if (context.defaultParallelism < 1) {
      1
    } else {
      context.defaultParallelism
    }
    val spaceConsumed = FileUtils.getSpaceOccupied(filePaths)
    val blockSize =
      hadoopConfiguration.getLongBytes("dfs.blocksize", CarbonCommonConstants.CARBON_256MB)
    LOGGER.info("[Block Distribution]")
    // calculate new block size to allow use all the parallelism
    if (spaceConsumed < defaultParallelism * blockSize) {
      var newSplitSize: Long = spaceConsumed / defaultParallelism
      if (newSplitSize < CarbonCommonConstants.CARBON_16MB) {
        newSplitSize = CarbonCommonConstants.CARBON_16MB
      }
      hadoopConfiguration.set(FileInputFormat.SPLIT_MAXSIZE, newSplitSize.toString)
      LOGGER.info(s"totalInputSpaceConsumed: $spaceConsumed , " +
          s"defaultParallelism: $defaultParallelism")
      LOGGER.info(s"mapreduce.input.fileinputformat.split.maxsize: ${ newSplitSize.toString }")
    }
  }
  private def newHadoopRDD(sc: SparkContext, location: String) = {
    val hadoopConfiguration = new Configuration(sc.hadoopConfiguration)
    hadoopConfiguration.setStrings(FileInputFormat.INPUT_DIR, location)
    hadoopConfiguration.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true)
    hadoopConfiguration.set("io.compression.codecs",
      """org.apache.hadoop.io.compress.GzipCodec,
         org.apache.hadoop.io.compress.DefaultCodec,
         org.apache.hadoop.io.compress.BZip2Codec""".stripMargin)

    configSplitMaxSize(sc, location, hadoopConfiguration)
    new NewHadoopRDD[LongWritable, Text](
      sc,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      hadoopConfiguration).setName("newHadoopRDD-spark-csv")
  }

  def withCharset(sc: SparkContext, location: String, charset: String): RDD[String] = {
    if (Charset.forName(charset) == TextFile.DEFAULT_CHARSET) {
      newHadoopRDD(sc, location).map(pair => pair._2.toString)
    } else {
      // can't pass a Charset object here cause its not serializable
      // TODO: maybe use mapPartitions instead?
      newHadoopRDD(sc, location).map(
        pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }
}
