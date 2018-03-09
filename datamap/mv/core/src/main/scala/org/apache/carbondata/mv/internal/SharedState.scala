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

package org.apache.carbondata.mv.internal

import java.io.File

import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.CacheManager

import org.apache.carbondata.mv.rewrite.SummaryDatasetCatalog

/**
 * A class that holds all state shared across sessions in a given [[SQLContext]].
 */
private[mv] class SharedState(val sparkContext: SparkContext) {

  @transient
  lazy val summaryDatasetCatalog = new SummaryDatasetCatalog(sparkContext)

  def clearSummaryDatasetCatalog(): Unit = summaryDatasetCatalog.clearSummaryDatasetCatalog()

  def registerSummaryDataset(query: DataFrame): Unit = {
    summaryDatasetCatalog.registerSummaryDataset(query)
  }

  def unregisterSummaryDataset(query: DataFrame): Unit = {
    summaryDatasetCatalog.unregisterSummaryDataset(query)
  }

  def refreshSummaryDatasetCatalog(filePath: String): Unit = {
    val file = new File(filePath)
    val sqlContext = new SQLContext(sparkContext)
    if (file.exists) {
      clearSummaryDatasetCatalog()
      for (line <- Source.fromFile(file).getLines) {
        registerSummaryDataset(sqlContext.sql(line))
      }
    }
  }

  /**
   * Class for caching query results reused in future executions.
   */
  val cacheManager: CacheManager = new CacheManager
}

object SharedState {}
