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

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import org.apache.carbondata.core.util.{SessionParams, ThreadLocalSessionParams}

/**
 * This RDD maintains session level ThreadLocal
 */
abstract class CarbonRDD[T: ClassTag](@transient sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]) extends RDD[T](sc, deps) {

  val sessionParams: SessionParams = ThreadLocalSessionParams.getSessionParams

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this (oneParent.context, List(new OneToOneDependency(oneParent)))

  // RDD compute logic should be here
  def internalCompute(split: Partition, context: TaskContext): Iterator[T]

  final def compute(split: Partition, context: TaskContext): Iterator[T] = {
    ThreadLocalSessionParams.setSessionParams(sessionParams)
    internalCompute(split, context)
  }
}
