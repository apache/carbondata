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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Dependency, OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util._
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil

/**
 * This RDD maintains session level ThreadLocal
 */
abstract class CarbonRDD[T: ClassTag](@transient sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]],
    @transient hadoopConf: Configuration) extends RDD[T](sc, deps) {

  val carbonSessionInfo: CarbonSessionInfo = {
    var info = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (info == null || info.getSessionParams == null) {
      info = new CarbonSessionInfo
      info.setSessionParams(new SessionParams())
    }
    info.getSessionParams.addProps(CarbonProperties.getInstance().getAddedProperty)
    info
  }

  private val confBytes = {
    val bao = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bao)
    hadoopConf.write(oos)
    oos.close()
    CompressorFactory.getInstance().getCompressor.compressByte(bao.toByteArray)
  }

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this (oneParent.context, List(new OneToOneDependency(oneParent)),
      oneParent.sparkContext.hadoopConfiguration)

  // RDD compute logic should be here
  def internalCompute(split: Partition, context: TaskContext): Iterator[T]

  final def compute(split: Partition, context: TaskContext): Iterator[T] = {
    CarbonInputFormatUtil.setS3Configurations(getConf)
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
    TaskMetricsMap.threadLocal.set(Thread.currentThread().getId)
    val carbonTaskInfo = new CarbonTaskInfo
    carbonTaskInfo.setTaskId(System.nanoTime)
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo)
    carbonSessionInfo.getSessionParams.getAddedProps.asScala.
      map(f => CarbonProperties.getInstance().addProperty(f._1, f._2))
    internalCompute(split, context)
  }

  def getConf: Configuration = {
    val configuration = new Configuration(false)
    val bai = new ByteArrayInputStream(CompressorFactory.getInstance().getCompressor
      .unCompressByte(confBytes))
    val ois = new ObjectInputStream(bai)
    configuration.readFields(ois)
    ois.close()
    configuration
  }
}

/**
 * This RDD contains TableInfo object which is serialized and deserialized in driver and executor
 */
abstract class CarbonRDDWithTableInfo[T: ClassTag](
    @transient sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]],
    serializedTableInfo: Array[Byte]) extends CarbonRDD[T](sc, deps, sc.hadoopConfiguration) {

  def this(@transient oneParent: RDD[_], serializedTableInfo: Array[Byte]) =
    this (oneParent.context, List(new OneToOneDependency(oneParent)), serializedTableInfo)

  def getTableInfo: TableInfo = TableInfo.deserialize(serializedTableInfo)
}
