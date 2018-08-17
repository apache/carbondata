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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport
import org.apache.carbondata.sdk.store.conf.StoreConf
import org.apache.carbondata.sdk.store.descriptor.{ScanDescriptor, TableIdentifier}
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl
import org.apache.carbondata.store.devapi.{InternalCarbonStore, InternalCarbonStoreFactory, Scanner, ScanOption}

/**
 * Carbon data source relation
 *
 * @param sparkSession
 * @param parameters
 * @param tableSchema
 */
case class CarbonSourceRelation(
                                 sparkSession: SparkSession,
                                 parameters: Map[String, String],
                                 tableSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan {

  lazy val store: InternalCarbonStore = {
    val storeConf = new StoreConf(System.getProperty("CARBON_STORE_CONF"))
    InternalCarbonStoreFactory.getStore("GlobalStore", storeConf)
  }

  val caseInsensitiveMap: Map[String, String] = parameters.map(f => (f._1.toLowerCase, f._2))

  private val tableIdentifier = new TableIdentifier(caseInsensitiveMap("tablename"),
    CarbonEnv.getDatabaseName(caseInsensitiveMap.get("dbname"))(sparkSession))

  CarbonSession.updateSessionInfoToCurrentThread(sparkSession)

  @transient lazy val carbonTable: CarbonTable = store.getCarbonTable(tableIdentifier)

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = tableSchema.getOrElse(getSparkSchema(carbonTable))

  private def getSparkSchema(sourceTable: CarbonTable): StructType = {
    val cols = sourceTable.getTableInfo.getFactTable.getListOfColumns.asScala.toArray
    val sortedCols = cols.filter(_.getSchemaOrdinal != -1)
      .sortWith(_.getSchemaOrdinal < _.getSchemaOrdinal)
    SparkDataTypeConverterImpl.convertToSparkSchema(sourceTable, sortedCols)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = new Array[Filter](0)

  override def toString: String = {
    "CarbonSourceRelation [ " + "Database name :" + tableIdentifier.getDatabaseName +
      ", " + "Table name :" + tableIdentifier.getTableName + ", Schema :" + tableSchema + " ]"
  }

  override def sizeInBytes: Long = 0L

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // create filter expression
    val filterExpression: Expression = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(schema, filter)
    }.reduceOption(new AndExpression(_, _)).orNull

    // create scan descriptor
    val scanDesc = ScanDescriptor.builder()
      .table(tableIdentifier)
      .select(requiredColumns)
      .filter(filterExpression)
      .create()

    // create scanner
    val scanOptions = new util.HashMap[String, String]()
    scanOptions.put(ScanOption.REMOTE_PRUNE, "true");
    scanOptions.put(ScanOption.OP_PUSHDOWN, "true")
    val scanner: Scanner[CarbonRow] =
      store.newScanner(
        tableIdentifier,
        scanDesc,
        scanOptions,
        classOf[SparkRowReadSupportImpl].asInstanceOf[Class[CarbonReadSupport[CarbonRow]]]
      )

    // create RDD
    new CarbonStoreScanRDD[Row, CarbonRow](sparkSession.sparkContext,
      tableIdentifier, scanner, filterExpression)
  }
}
