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

package org.apache.carbondata.spark.rdd

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Dependency, OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util._

/**
 * This RDD maintains session level ThreadLocal
 */
abstract class CarbonRDD[T: ClassTag](
    @transient private val ss: SparkSession,
    @transient private var deps: Seq[Dependency[_]]) extends RDD[T](ss.sparkContext, deps) {

  @transient val sparkAppName: String = ss.sparkContext.appName
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, sparkAppName)

  val carbonSessionInfo: CarbonSessionInfo = {
    var info = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (info == null || info.getSessionParams == null) {
      info = new CarbonSessionInfo
      info.setSessionParams(new SessionParams())
    }
    info.getSessionParams.addProps(CarbonProperties.getInstance().getAddedProperty)
    info
  }

  val inputMetricsInterval: Long = CarbonProperties.getInputMetricsInterval

  @transient val hadoopConf = SparkSQLUtil.sessionState(ss).newHadoopConf()

  val config = SparkSQLUtil.broadCastHadoopConf(sparkContext, hadoopConf)

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient sparkSession: SparkSession, @transient oneParent: RDD[_]) =
    this (sparkSession, List(new OneToOneDependency(oneParent)))

  protected def internalGetPartitions: Array[Partition]

  override def getPartitions: Array[Partition] = {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(hadoopConf)
    internalGetPartitions
  }

  // RDD compute logic should be here
  def internalCompute(split: Partition, context: TaskContext): Iterator[T]

  final def compute(split: Partition, context: TaskContext): Iterator[T] = {
    TaskContext.get.addTaskCompletionListener(_ => ThreadLocalSessionInfo.unsetAll())
    carbonSessionInfo.getNonSerializableExtraInfo.put("carbonConf", getConf)
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
    TaskMetricsMap.initializeThreadLocal()
    val carbonTaskInfo = new CarbonTaskInfo
    carbonTaskInfo.setTaskId(CarbonUtil.generateUUID())
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo)
    carbonSessionInfo.getSessionParams.getAddedProps.asScala.
      map(f => CarbonProperties.getInstance().addProperty(f._1, f._2))
    internalCompute(split, context)
  }

  def getConf: Configuration = {
    config.value.value
  }
}

/**
 * This RDD contains TableInfo object which is serialized and deserialized in driver and executor
 */
abstract class CarbonRDDWithTableInfo[T: ClassTag](
    @transient private val ss: SparkSession,
    @transient private var deps: Seq[Dependency[_]],
    serializedTableInfo: Array[Byte]) extends CarbonRDD[T](ss, deps) {

  def this(@transient sparkSession: SparkSession, @transient oneParent: RDD[_],
      serializedTableInfo: Array[Byte]) = {
    this (sparkSession, List(new OneToOneDependency(oneParent)), serializedTableInfo)
  }

  def getTableInfo: TableInfo = TableInfo.deserialize(serializedTableInfo)
}
