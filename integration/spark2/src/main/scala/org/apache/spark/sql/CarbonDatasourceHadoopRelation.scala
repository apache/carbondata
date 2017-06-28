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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.LoadTableByInsert
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.util.{CarbonSessionInfo, SessionParams, ThreadLocalSessionInfo}
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.hadoop.util.SchemaReader
import org.apache.carbondata.processing.merger.TableMeta
import org.apache.carbondata.spark.CarbonFilters
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.apache.carbondata.spark.util.CarbonSparkUtil

case class CarbonDatasourceHadoopRelation(
    sparkSession: SparkSession,
    paths: Array[String],
    parameters: Map[String, String],
    tableSchema: Option[StructType],
    isSubquery: ArrayBuffer[Boolean] = new ArrayBuffer[Boolean]())
  extends BaseRelation with InsertableRelation {

  lazy val absIdentifier = AbsoluteTableIdentifier.fromTablePath(paths.head)
  lazy val carbonTable = carbonRelation.tableMeta.carbonTable
  lazy val carbonRelation: CarbonRelation = CarbonEnv.getInstance(sparkSession).carbonMetastore
    .lookupRelation(Some(absIdentifier.getCarbonTableIdentifier.getDatabaseName),
      absIdentifier.getCarbonTableIdentifier.getTableName)(sparkSession)
    .asInstanceOf[CarbonRelation]

  val carbonSessionInfo : CarbonSessionInfo = CarbonEnv.getInstance(sparkSession).carbonSessionInfo
  ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = tableSchema.getOrElse(carbonRelation.schema)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[InternalRow] = {
    val filterExpression: Option[Expression] = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(schema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = new CarbonProjection
    requiredColumns.foreach(projection.addColumn)

    new CarbonScanRDD(sqlContext.sparkContext, projection, filterExpression.orNull,
      absIdentifier, carbonTable)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = new Array[Filter](0)

  override def toString: String = {
    "CarbonDatasourceHadoopRelation [ " + "Database name :" + carbonTable.getDatabaseName +
    ", " + "Table name :" + carbonTable.getFactTableName + ", Schema :" + tableSchema + " ]"
  }

  override def sizeInBytes: Long = carbonRelation.sizeInBytes

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (carbonRelation.output.size > CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      sys.error("Maximum supported column by carbon is: " +
          CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS)
    }
    if (data.logicalPlan.output.size >= carbonRelation.output.size) {
      LoadTableByInsert(this, data.logicalPlan).run(sparkSession)
    } else {
      sys.error("Cannot insert into target table because column number are different")
    }
  }

}
