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

package org.apache.carbondata.spark.util

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier
import org.apache.carbondata.hadoop.CarbonInputFormat


/**
 * All the utility functions for carbon plan creation
 */
object QueryPlanUtil {

  /**
   * createCarbonInputFormat from query model
   */
  def createCarbonInputFormat(absoluteTableIdentifier: AbsoluteTableIdentifier) :
  (CarbonInputFormat[Array[Object]], Job) = {
    val carbonInputFormat = new CarbonInputFormat[Array[Object]]()
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    (carbonInputFormat, job)
  }

  def createCarbonInputFormat[V: ClassTag](absoluteTableIdentifier: AbsoluteTableIdentifier,
      conf: Configuration) : CarbonInputFormat[V] = {
    val carbonInputFormat = new CarbonInputFormat[V]()
    val job: Job = new Job(conf)
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    carbonInputFormat
  }
}
