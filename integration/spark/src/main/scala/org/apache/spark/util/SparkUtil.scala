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

package org.apache.spark.util

import scala.collection.JavaConverters.{mapAsScalaMapConverter, _}

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SPARK_VERSION, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.command.mutation.merge.MergeAction
import org.apache.spark.sql.types._

/*
 * this object use to handle file splits
 */
object SparkUtil {

  def convertMap(map: java.util.Map[Column, Column]): Map[Column, Column] = {
    map.asScala.toMap
  }

  def convertExpressionList(list: java.util.List[Expression]): List[Expression] = {
    list.asScala.toList
  }

  def convertMergeActionList(list: java.util.List[MergeAction]): List[MergeAction] = {
    list.asScala.toList
  }

  def setTaskContext(context: TaskContext): Unit = {
    val localThreadContext = TaskContext.get()
    if (localThreadContext == null) {
      TaskContext.setTaskContext(context)
    }
  }

  /**
   * Utility method to compare the Spark Versions.
   * This API ignores the sub-version and compares with only major version
   * Version passed should be of format x.y  e.g 2.2 ,2.3 , SPARK_VERSION
   * will be of format x.y.z e.g 2.3.0,2.2.1
   */
  def isSparkVersionXAndAbove(xVersion: String, isEqualComparision: Boolean = false): Boolean = {
    val tmpArray = SPARK_VERSION.split("\\.")
    // convert to float
    val sparkVersion = if (tmpArray.length >= 2) {
      (tmpArray(0) + "." + tmpArray(1)).toFloat
    } else {
      (tmpArray(0) + ".0").toFloat
    }
    // compare the versions
    if (isEqualComparision) {
      sparkVersion == xVersion.toFloat
    } else {
      sparkVersion >= xVersion.toFloat
    }
  }

  def isSparkVersionEqualTo(xVersion: String): Boolean = {
    isSparkVersionXAndAbove(xVersion, true)
  }

  def setNullExecutionId(sparkSession: SparkSession): Unit = {
    // "spark.sql.execution.id is already set" exception will be
    // thrown if not set to null in spark2.2 and below versions
    if (!SparkUtil.isSparkVersionXAndAbove("2.3")) {
      sparkSession.sparkContext.setLocalProperty(EXECUTION_ID_KEY, null)
    }
  }

  def addCredentials(conf: JobConf) {
    SparkHadoopUtil.get.addCredentials(conf)
  }

  def isPrimitiveType(datatype: DataType): Boolean = {
    datatype match {
      case StringType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case BinaryType => true
      case BooleanType => true
      case DateType => true
      case TimestampType => true
      case DecimalType() => true
      case _ => false
    }
  }
}
