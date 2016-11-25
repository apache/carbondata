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

import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hadoop.CarbonInputFormat

// scalastyle:off println
object HadoopFileExample {

  def main(args: Array[String]): Unit = {
    val cc = ExampleUtils.createCarbonContext("DataFrameAPIExample")
    ExampleUtils.writeSampleCarbonFile(cc, "carbon1")

    val sc = cc.sparkContext
    val input = sc.newAPIHadoopFile(s"${cc.storePath}/default/carbon1",
      classOf[CarbonInputFormat[Array[Object]]],
      classOf[Void],
      classOf[Array[Object]])
    val result = input.map(x => x._2.toList).collect
    result.foreach(x => println(x.mkString(", ")))
  }
}
// scalastyle:on println

