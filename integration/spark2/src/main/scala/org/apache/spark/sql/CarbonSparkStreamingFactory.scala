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

package org.apache.spark.sql

import org.apache.commons.lang3.StringUtils

import org.apache.carbondata.streaming.CarbonStreamException
import org.apache.carbondata.streaming.CarbonStreamSparkStreaming
import org.apache.carbondata.streaming.CarbonStreamSparkStreamingWriter

/**
 * Create [[CarbonStreamSparkStreamingWriter]] for stream table
 * when integrate with Spark Streaming.
 *
 * NOTE: Current integration with Spark Streaming is an alpha feature.
 */
object CarbonSparkStreamingFactory {

  def getStreamSparkStreamingWriter(spark: SparkSession,
    dbNameStr: String,
    tableName: String): CarbonStreamSparkStreamingWriter =
    synchronized {
    val dbName = if (StringUtils.isEmpty(dbNameStr)) "default" else dbNameStr
    val key = dbName + "." + tableName
    if (CarbonStreamSparkStreaming.getTableMap.containsKey(key)) {
      CarbonStreamSparkStreaming.getTableMap.get(key)
    } else {
      if (StringUtils.isEmpty(tableName) || tableName.contains(" ")) {
        throw new CarbonStreamException("Table creation failed. " +
                                        "Table name must not be blank or " +
                                        "cannot contain blank space")
      }
      val carbonTable = CarbonEnv.getCarbonTable(Some(dbName),
        tableName)(spark)
      if (!carbonTable.isStreamingSink) {
        throw new CarbonStreamException(s"Table ${carbonTable.getDatabaseName}." +
                                        s"${carbonTable.getTableName} is not a streaming table")
      }
      val streamWriter = new CarbonStreamSparkStreamingWriter(spark,
        carbonTable, spark.sessionState.newHadoopConf())
      CarbonStreamSparkStreaming.getTableMap.put(key, streamWriter)
      streamWriter
    }
  }
}
