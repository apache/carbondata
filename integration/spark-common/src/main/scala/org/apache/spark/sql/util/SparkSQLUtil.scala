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

import java.lang.reflect.Method

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EmptyRule
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSeq, Cast, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.util.{CarbonReflectionUtils, SerializableConfiguration, SparkUtil, Utils}

object SparkSQLUtil {
  def sessionState(sparkSession: SparkSession): SessionState = sparkSession.sessionState

  def execute(logicalPlan: LogicalPlan, sparkSession: SparkSession): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def getSparkSession: SparkSession = {
    SparkSession.getDefaultSession.get
  }

  def invokeStatsMethod(logicalPlanObj: LogicalPlan, conf: SQLConf): Statistics = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val method: Method = logicalPlanObj.getClass.getMethod("stats", classOf[SQLConf])
      method.invoke(logicalPlanObj, conf).asInstanceOf[Statistics]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val method: Method = logicalPlanObj.getClass.getMethod("stats")
      method.invoke(logicalPlanObj).asInstanceOf[Statistics]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def invokeQueryPlannormalizeExprId(r: NamedExpression, input: AttributeSeq)
      : NamedExpression = {
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      val clazz = Utils.classForName("org.apache.spark.sql.catalyst.plans.QueryPlan")
      clazz.getDeclaredMethod("normalizeExprId", classOf[Any], classOf[AttributeSeq]).
        invoke(null, r, input).asInstanceOf[NamedExpression]
    } else {
      r
    }
  }

  def getStatisticsObj(outputList: Seq[NamedExpression],
                       plan: LogicalPlan, stats: Statistics,
                       aliasMap: Option[AttributeMap[Attribute]] = None)
  : Statistics = {
    val className = "org.apache.spark.sql.catalyst.plans.logical.Statistics"
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      val output = outputList.map(_.toAttribute)
      val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
        table => AttributeMap(table.output.zip(output))
      }
      val rewrites = mapSeq.head
      val attributes : AttributeMap[ColumnStat] = CarbonReflectionUtils.
        getField("attributeStats", stats).asInstanceOf[AttributeMap[ColumnStat]]
      var attributeStats = AttributeMap(attributes.iterator
        .map { pair => (rewrites(pair._1), pair._2) }.toSeq)
      if (aliasMap.isDefined) {
        attributeStats = AttributeMap(
          attributeStats.map(pair => (aliasMap.get(pair._1), pair._2)).toSeq)
      }
      val hints = CarbonReflectionUtils.getField("hints", stats).asInstanceOf[Object]
      CarbonReflectionUtils.createObject(className, stats.sizeInBytes,
        stats.rowCount, attributeStats, hints).asInstanceOf[Statistics]
    } else {
      val output = outputList.map(_.name)
      val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
        table => table.output.map(_.name).zip(output).toMap
      }
      val rewrites = mapSeq.head
      val colStats = CarbonReflectionUtils.getField("colStats", stats)
        .asInstanceOf[Map[String, ColumnStat]]
      var attributeStats = colStats.iterator
        .map { pair => (rewrites(pair._1), pair._2) }.toMap
      if (aliasMap.isDefined) {
        val aliasMapName = aliasMap.get.map(x => (x._1.name, x._2.name))
        attributeStats =
          attributeStats.map(pair => (aliasMapName.getOrElse(pair._1, pair._1)
            , pair._2))
      }
      CarbonReflectionUtils.createObject(className, stats.sizeInBytes,
        stats.rowCount, attributeStats).asInstanceOf[Statistics]
    }
  }

  def getEliminateViewObj(): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      val className = "org.apache.spark.sql.catalyst.analysis.EliminateView"
      CarbonReflectionUtils.createSingleObject(className).asInstanceOf[Rule[LogicalPlan]]
    } else {
      EmptyRule
    }
  }

  def getPullupCorrelatedPredicatesObj(): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.PullupCorrelatedPredicates"
      CarbonReflectionUtils.createSingleObject(className).asInstanceOf[Rule[LogicalPlan]]
    } else {
      EmptyRule
    }
  }

  def getRemoveRedundantAliasesObj(): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAliases"
      CarbonReflectionUtils.createSingleObject(className).asInstanceOf[Rule[LogicalPlan]]
    } else {
      EmptyRule
    }
  }

  def getReorderJoinObj(conf: SQLConf): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.ReorderJoin";
      CarbonReflectionUtils.createObject(className, conf)._1.asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.ReorderJoin$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.ReorderJoin$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    }
    else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def getEliminateOuterJoinObj(conf: SQLConf): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin";
      CarbonReflectionUtils.createObject(className, conf)._1.asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.EliminateOuterJoin$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    }
    else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def getNullPropagationObj(conf: SQLConf): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.NullPropagation";
      CarbonReflectionUtils.createObject(className, conf)._1.asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.NullPropagation$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.NullPropagation$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def getCheckCartesianProductsObj(conf: SQLConf): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.CheckCartesianProducts";
      CarbonReflectionUtils.createObject(className, conf)._1.asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.CheckCartesianProducts$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.CheckCartesianProducts";
      CarbonReflectionUtils.createObject(className, conf)._1.asInstanceOf[Rule[LogicalPlan]]
    }
    else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
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

}
