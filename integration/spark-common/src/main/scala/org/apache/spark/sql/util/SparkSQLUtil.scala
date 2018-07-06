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

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

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

  def getReorderJoinObj(conf: SQLConf): Rule[LogicalPlan] = {
    if (SparkUtil.isSparkVersionEqualTo("2.2")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.ReorderJoin";
      CarbonReflectionUtils.createObject(className, conf)._1.asInstanceOf[Rule[LogicalPlan]]
    } else if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val className = "org.apache.spark.sql.catalyst.optimizer.ReorderJoin$";
      CarbonReflectionUtils.createObjectOfPrivateConstructor(className)._1
        .asInstanceOf[Rule[LogicalPlan]]
    } else {
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
    } else {
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
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }
}
