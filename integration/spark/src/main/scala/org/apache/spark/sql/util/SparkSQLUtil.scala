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

package org.apache.spark.sql.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonToSparkAdapter, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateView
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{CheckCartesianProducts, EliminateOuterJoin, NullPropagation, PullupCorrelatedPredicates, RemoveRedundantAliases, ReorderJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Statistics, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.DataLoadMetrics

object SparkSQLUtil {
  def sessionState(sparkSession: SparkSession): SessionState = sparkSession.sessionState

  def execute(logicalPlan: LogicalPlan, sparkSession: SparkSession): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def execute(rdd: RDD[InternalRow], schema: StructType, sparkSession: SparkSession): DataFrame = {
    execute(LogicalRDD(schema.toAttributes, rdd)(sparkSession), sparkSession)
  }

  def getSparkSession: SparkSession = {
    SparkSession.getActiveSession.orNull
  }

  def invokeStatsMethod(logicalPlanObj: LogicalPlan, conf: SQLConf): Statistics = {
    logicalPlanObj.stats
  }

  def invokeQueryPlanNormalizeExprId(r: NamedExpression, input: AttributeSeq)
      : NamedExpression = {
    CarbonToSparkAdapter.normalizeExpressions(r, input)
  }

  def getEliminateViewObj(): Rule[LogicalPlan] = {
    EliminateView
  }

  def getPullUpCorrelatedPredicatesObj(): Rule[LogicalPlan] = {
    PullupCorrelatedPredicates
  }

  def getRemoveRedundantAliasesObj(): Rule[LogicalPlan] = {
    RemoveRedundantAliases
  }

  def getReorderJoinObj(conf: SQLConf): Rule[LogicalPlan] = {
    ReorderJoin
  }

  def getEliminateOuterJoinObj(conf: SQLConf): Rule[LogicalPlan] = {
    EliminateOuterJoin
  }

  def getNullPropagationObj(conf: SQLConf): Rule[LogicalPlan] = {
    NullPropagation
  }

  def getCheckCartesianProductsObj(conf: SQLConf): Rule[LogicalPlan] = {
    CheckCartesianProducts
  }

  /**
   * Method to broadcast a variable using spark SerializableConfiguration class
   *
   * @param sparkContext
   * @param hadoopConf
   * @return
   */
  def broadCastHadoopConf(sparkContext: SparkContext,
      hadoopConf: Configuration): Broadcast[SerializableConfiguration] = {
    sparkContext.broadcast(getSerializableConfigurableInstance(hadoopConf))
  }

  def getSerializableConfigurableInstance(hadoopConf: Configuration): SerializableConfiguration = {
    new SerializableConfiguration(hadoopConf)
  }

  /**
   * Get the task group id
   *
   * @param sparkSession
   * @return
   */
  def getTaskGroupId(sparkSession: SparkSession): String = {
    val taskGroupId = sparkSession.sparkContext.getLocalProperty("spark.jobGroup.id") match {
      case null => ""
      case _ => sparkSession.sparkContext.getLocalProperty("spark.jobGroup.id")
    }
    taskGroupId
  }

  /**
   * Get the task group description
   *
   * @param sparkSession
   * @return
   */
  def getTaskGroupDesc(sparkSession: SparkSession): String = {
    val taskGroupDesc = sparkSession.sparkContext.getLocalProperty("spark.job.description") match {
      case null => ""
      case _ => sparkSession.sparkContext.getLocalProperty("spark.job.description")
    }
    taskGroupDesc
  }

  /**
   * create DataFrame based on carbonTable
   */
  def createInputDataFrame(
      sparkSession: SparkSession,
      carbonTable: CarbonTable): DataFrame = {
    /**
     * [[org.apache.spark.sql.catalyst.expressions.objects.ValidateExternalType]] validates the
     * datatype of column data and corresponding datatype in schema provided to create DataFrame.
     * Since carbonScanRDD gives Long data for timestamp column and corresponding column datatype in
     * schema is Timestamp, this validation fails if we use createDataFrame API which takes rdd as
     * input. Hence, using below API which creates DataFrame from qualified table name.
     */
    sparkSession.sqlContext.table(carbonTable.getDatabaseName + "." + carbonTable.getTableName)
  }

  def setOutputMetrics(outputMetrics: OutputMetrics, dataLoadMetrics: DataLoadMetrics): Unit = {
    if (dataLoadMetrics != null && outputMetrics != null) {
      outputMetrics.setBytesWritten(dataLoadMetrics.getNumOutputBytes)
      outputMetrics.setRecordsWritten(dataLoadMetrics.getNumOutputRows)
    }
  }

  def isCommand(logicalPlan: LogicalPlan): Boolean = logicalPlan match {
    case _: Command => true
    case u: Union if u.children.forall(_.isInstanceOf[Command]) => true
    case _ => false
  }

  def isRelation(className: String): Boolean = {
    className.equals("org.apache.spark.sql.catalyst.catalog.CatalogRelation") ||
    className.equals("org.apache.spark.sql.catalyst.catalog.HiveTableRelation") ||
    className.equals("org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation")
  }
}
