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
import org.apache.spark.{Dependency, OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, CarbonSessionInfo, CarbonTaskInfo, SessionParams, TaskMetricsMap, ThreadLocalSessionInfo, ThreadLocalTaskInfo}

/**
 * This RDD maintains session level ThreadLocal
 */
abstract class CarbonRDD[T: ClassTag](@transient sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]],
    @transient configuration: Configuration = null) extends RDD[T](sc, deps) {

  private val confBytes = SparkUtil.compressConfiguration(configuration)

  def getConf(): Configuration = {
    SparkUtil.uncompressConfiguration(confBytes)
  }

  val carbonSessionInfo: CarbonSessionInfo = {
    var info = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (info == null || info.getSessionParams == null) {
      info = new CarbonSessionInfo
      info.setSessionParams(new SessionParams())
    }
    info.getSessionParams.addProps(CarbonProperties.getInstance().getAddedProperty)
    info
  }

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this (oneParent.context, List(new OneToOneDependency(oneParent)), null)

  def this(@transient oneParent: RDD[_], @transient hadoopConf: Configuration) =
    this (oneParent.context, List(new OneToOneDependency(oneParent)), hadoopConf)

  // RDD compute logic should be here
  def internalCompute(split: Partition, context: TaskContext): Iterator[T]

  final def compute(split: Partition, context: TaskContext): Iterator[T] = {
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
    TaskMetricsMap.threadLocal.set(Thread.currentThread().getId)
    val carbonTaskInfo = new CarbonTaskInfo
    carbonTaskInfo.setTaskId(System.nanoTime)
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo)
    carbonSessionInfo.getSessionParams.getAddedProps.asScala.
      map(f => CarbonProperties.getInstance().addProperty(f._1, f._2))
    internalCompute(split, context)
  }
}

/**
 * This RDD contains TableInfo object which is serialized and deserialized in driver and executor
 */
abstract class CarbonRDDWithTableInfo[T: ClassTag](
    @transient sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]],
    serializedTableInfo: Array[Byte],
    @transient configuration: Configuration) extends CarbonRDD[T](sc, deps, configuration) {

  def this(@transient oneParent: RDD[_], serializedTableInfo: Array[Byte],
      @transient configuration: Configuration) =
    this (oneParent.context, List(new OneToOneDependency(oneParent)), serializedTableInfo,
      configuration)

  def getTableInfo: TableInfo = TableInfo.deserialize(serializedTableInfo)
}
