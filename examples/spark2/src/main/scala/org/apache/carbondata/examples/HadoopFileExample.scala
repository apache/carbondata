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

import org.apache.hadoop.conf.Configuration

import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonProjection}

// scalastyle:off println
object HadoopFileExample {

  def main(args: Array[String]): Unit = {
    val spark = ExampleUtils.createCarbonSession("HadoopFileExample")
    ExampleUtils.writeSampleCarbonFile(spark, "carbon1")

    // read two columns
    val projection = new CarbonProjection
    projection.addColumn("c1")  // column c1
    projection.addColumn("c3")  // column c3
    val conf = new Configuration()
    CarbonInputFormat.setColumnProjection(conf, projection)

    val sc = spark.sparkContext
    val input = sc.newAPIHadoopFile(s"${ExampleUtils.storeLocation}/default/carbon1",
      classOf[CarbonInputFormat[Array[Object]]],
      classOf[Void],
      classOf[Array[Object]],
      conf)
    val result = input.map(x => x._2.toList).collect
    result.foreach(x => println(x.mkString(", ")))

    // delete carbondata file
    ExampleUtils.cleanSampleCarbonFile(spark, "carbon1")
  }
}
// scalastyle:on println

