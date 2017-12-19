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

import org.apache.spark.CarbonInputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class CarbonDatasourceHadoopRelation(
    sparkSession: SparkSession,
    paths: Array[String],
    parameters: Map[String, String],
    tableSchema: Option[StructType],
    isSubquery: ArrayBuffer[Boolean] = new ArrayBuffer[Boolean]())
  extends BaseRelation with InsertableRelation {

  var caseInsensitiveMap = parameters.map(f => (f._1.toLowerCase, f._2))
  lazy val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
    paths.head,
    CarbonEnv.getDatabaseName(caseInsensitiveMap.get("dbname"))(sparkSession),
    caseInsensitiveMap("tablename"))
  lazy val databaseName: String = carbonTable.getDatabaseName
  lazy val tableName: String = carbonTable.getTableName
  CarbonSession.updateSessionInfoToCurrentThread(sparkSession)

  @transient lazy val carbonRelation: CarbonRelation =
    CarbonEnv.getInstance(sparkSession).carbonMetastore.
    createCarbonRelation(parameters, identifier, sparkSession)


  @transient lazy val carbonTable: CarbonTable = carbonRelation.carbonTable

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = tableSchema.getOrElse(carbonRelation.schema)

  def buildScan(requiredColumns: Array[String],
      filters: Array[Filter],
      partitions: Seq[String]): RDD[InternalRow] = {
    val filterExpression: Option[Expression] = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(schema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = new CarbonProjection
    requiredColumns.foreach(projection.addColumn)
    val inputMetricsStats: CarbonInputMetrics = new CarbonInputMetrics
    new CarbonScanRDD(
      sparkSession.sparkContext,
      projection,
      filterExpression.orNull,
      identifier,
      carbonTable.getTableInfo.serialize(),
      carbonTable.getTableInfo,
      inputMetricsStats,
      partitions)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = new Array[Filter](0)

  override def toString: String = {
    "CarbonDatasourceHadoopRelation [ " + "Database name :" + databaseName +
    ", " + "Table name :" + tableName + ", Schema :" + tableSchema + " ]"
  }

  override def sizeInBytes: Long = carbonRelation.sizeInBytes

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (carbonRelation.output.size > CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      CarbonException.analysisException("Maximum supported column by carbon is: " +
        CarbonCommonConstants.DEFAULT_MAX_NUMBER_OF_COLUMNS)
    }
    if (data.logicalPlan.output.size >= carbonRelation.output.size) {
      CarbonInsertIntoCommand(this, data.logicalPlan, overwrite, Map.empty).run(sparkSession)
    } else {
      CarbonException.analysisException(
        "Cannot insert into target table because number of columns mismatch")
    }
  }

}
