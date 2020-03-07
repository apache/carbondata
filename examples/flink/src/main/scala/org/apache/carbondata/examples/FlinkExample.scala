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

package org.apache.carbondata.examples

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}

// Write carbondata file by spark and read it by flink
// scalastyle:off println
object FlinkExample {

  def main(args: Array[String]): Unit = {
    // write carbondata file by spark
    val cc = ExampleUtils.createCarbonSession("FlinkExample")
    val path = ExampleUtils.writeSampleCarbonFile(cc, "carbon1")

    // read two columns by flink
    val projection = new CarbonProjection
    projection.addColumn("c1")  // column c1
    projection.addColumn("c3")  // column c3
    val conf = new Configuration()
    CarbonInputFormat.setColumnProjection(conf, projection)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.readHadoopFile(
      new CarbonTableInputFormat[Array[Object]],
      classOf[Void],
      classOf[Array[Object]],
      path,
      new Job(conf)
    )

    // print result
    val result = ds.collect()
    for (i <- 0 until result.size()) {
      println(result.get(i).f1.mkString(","))
    }

    // delete carbondata file
    ExampleUtils.cleanSampleCarbonFile(cc, "carbon1")
  }
}
// scalastyle:on println
