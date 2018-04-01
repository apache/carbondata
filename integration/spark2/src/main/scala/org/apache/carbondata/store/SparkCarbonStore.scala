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

package org.apache.carbondata.store

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.spark.{CarbonInputMetrics, SparkConf}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.annotations.InterfaceAudience
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * A CarbonStore implementation that uses Spark as underlying compute engine
 * with CarbonData query optimization capability
 */
@InterfaceAudience.Internal
private[store] class SparkCarbonStore extends MetaCachedCarbonStore {
  private var session: SparkSession = _

  /**
   * Initialize SparkCarbonStore
   * @param storeName store name
   * @param storeLocation location to store data
   */
  def this(storeName: String, storeLocation: String) = {
    this()
    val sparkConf = new SparkConf(loadDefaults = true)
    session = SparkSession.builder
      .config(sparkConf)
      .appName("SparkCarbonStore-" + storeName)
      .config("spark.sql.warehouse.dir", storeLocation)
      .getOrCreateCarbonSession()
  }

  @throws[IOException]
  override def scan(
      path: String,
      projectColumns: Array[String]): java.util.Iterator[CarbonRow] = {
    scan(path, projectColumns, null)
  }

  @throws[IOException]
  override def scan(
      path: String,
      projectColumns: Array[String],
      filter: Expression): java.util.Iterator[CarbonRow] = {
    require(path != null)
    require(projectColumns != null)
    val table = getTable(path)
    val rdd = new CarbonScanRDD[CarbonRow](
      spark = session,
      columnProjection = new CarbonProjection(projectColumns),
      filterExpression = filter,
      identifier = table.getAbsoluteTableIdentifier,
      serializedTableInfo = table.getTableInfo.serialize,
      tableInfo = table.getTableInfo,
      inputMetricsStats = new CarbonInputMetrics,
      partitionNames = null,
      dataTypeConverterClz = null,
      readSupportClz = classOf[CarbonRowReadSupport])
    rdd.collect
      .iterator
      .asJava
  }

  @throws[IOException]
  override def sql(sqlString: String): java.util.Iterator[CarbonRow] = {
    val df = session.sql(sqlString)
    df.rdd
      .map(row => new CarbonRow(row.toSeq.toArray.asInstanceOf[Array[Object]]))
      .collect()
      .iterator
      .asJava
  }

}
