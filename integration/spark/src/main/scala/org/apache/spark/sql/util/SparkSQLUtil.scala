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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateView
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSeq, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.{CheckCartesianProducts, EliminateOuterJoin, NullPropagation, PullupCorrelatedPredicates, RemoveRedundantAliases, ReorderJoin}
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

object SparkSQLUtil {
  def sessionState(sparkSession: SparkSession): SessionState = sparkSession.sessionState

  def execute(logicalPlan: LogicalPlan, sparkSession: SparkSession): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def execute(rdd: RDD[InternalRow], schema: StructType, sparkSession: SparkSession): DataFrame = {
    execute(LogicalRDD(schema.toAttributes, rdd)(sparkSession), sparkSession)
  }

  def getSparkSession: SparkSession = {
    SparkSession.getActiveSession.getOrElse(null)
  }

  def invokeStatsMethod(logicalPlanObj: LogicalPlan, conf: SQLConf): Statistics = {
    logicalPlanObj.stats
  }

  def invokeQueryPlannormalizeExprId(r: NamedExpression, input: AttributeSeq)
      : NamedExpression = {
    QueryPlan.normalizeExprId(r, input)
  }

  def getStatisticsObj(outputList: Seq[NamedExpression],
                       plan: LogicalPlan, stats: Statistics,
                       aliasMap: Option[AttributeMap[Attribute]] = None)
  : Statistics = {
    val output = outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq.head
    val attributes : AttributeMap[ColumnStat] = stats.attributeStats
    var attributeStats = AttributeMap(attributes.iterator
      .map { pair => (rewrites(pair._1), pair._2) }.toSeq)
    if (aliasMap.isDefined) {
      attributeStats = AttributeMap(
        attributeStats.map(pair => (aliasMap.get(pair._1), pair._2)).toSeq)
    }
    val hints = stats.hints
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats, hints)
  }

  def getEliminateViewObj(): Rule[LogicalPlan] = {
    EliminateView
  }

  def getPullupCorrelatedPredicatesObj(): Rule[LogicalPlan] = {
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
     * datatype of column data and corresponding datatype in schema provided to create dataframe.
     * Since carbonScanRDD gives Long data for timestamp column and corresponding column datatype in
     * schema is Timestamp, this validation fails if we use createDataFrame API which takes rdd as
     * input. Hence, using below API which creates dataframe from tablename.
     */
    sparkSession.sqlContext.table(carbonTable.getTableName)
  }
}
