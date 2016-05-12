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

package org.carbondata.integration.spark.thriftserver

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.CarbonContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

import org.carbondata.core.util.CarbonProperties

object CarbonThriftServer {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("Carbon Thrift Server")
    val sparkHome = System.getenv.get("SPARK_HOME")
    if (null != sparkHome) {
      conf.set("carbon.properties.filepath", sparkHome + '/' + "conf" + '/' + "carbon.properties")
      System.setProperty("carbon.properties.filepath",
        sparkHome + '/' + "conf" + '/' + "carbon.properties")
    }
    val sc = new SparkContext(conf);
    val warmUpTime = CarbonProperties.getInstance().getProperty("carbon.spark.warmUpTime", "5000");
    try {
      Thread.sleep(Integer.parseInt(warmUpTime));
    } catch {
      case _ =>
        // scalastyle:off
        println("Wrong value for carbon.spark.warmUpTime " + warmUpTime +
          "Using default Value and proceeding");
        // scalastyle:on
        Thread.sleep(30000);
    }

    val carbonContext = new CarbonContext(sc, args(0))

    HiveThriftServer2.startWithContext(carbonContext)
  }

}
