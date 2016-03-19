/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
  *
  */
package com.huawei.datasight.spark.thriftserver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.OlapContext
import com.huawei.unibi.molap.util.MolapProperties
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

object MolapThriftServer {

  def main(args: Array[String]): Unit = {
    args.foreach(println)
    var conf = new SparkConf()
      .setMaster(args(0))
      .set("spark.executor.memory", args(1))
      .set("spark.cores.max", args(2))
      .set("spark.eventLog.enabled", "false")
      .setAppName("Molap Thrift Server")
      .set("spark.hadoop.dfs.client.domain.socket.data.traffic", "false")
      .set("spark.hadoop.dfs.client.read.shortcircuit", "true")
      .set("spark.hadoop.dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket")
      .set("spark.hadoop.dfs.block.local-path-access.user", "root,hadoop")
      .set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
      .set("spark.kryo.registrator", "com.huawei.datasight.spark.MyRegistrator")
      .set("spark.sql.useSerializer2", "false")
      .set("spark.kryoserializer.buffer", "100k")
    val sparkHome = System.getenv.get("SPARK_HOME")
    print("sparkHome: " + sparkHome)
    if (null != sparkHome) {
      conf.set("molap.properties.filepath", sparkHome + '/' + "conf" + '/' + "molap.properties")
      System.setProperty("molap.properties.filepath", sparkHome + '/' + "conf" + '/' + "molap.properties")
    }
    print("Molap Property file path: " + conf.get("molap.properties.filepath"))
    val sc = new SparkContext(conf);
    val warmUpTime = MolapProperties.getInstance().getProperty("molap.spark.warmUpTime", "5000");
    println("Sleeping for millisecs:" + warmUpTime);
    try {
      Thread.sleep(Integer.parseInt(warmUpTime));
    } catch {
      case _ => {
        println("Wrong value for molap.spark.warmUpTime " + warmUpTime + "Using default Value and proceeding"); Thread.sleep(30000);
      }
    }

    val olapContext = new OlapContext(sc, args(3))
    olapContext.setConf("spark.sql.shuffle.partitions", "40")

    //Note.Uncomment the below line and change OlapContext such that it extends Hive Context
    HiveThriftServer2.startWithContext(olapContext)
  }

}
