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

package org.carbondata.examples

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, SaveMode}

import org.carbondata.core.util.CarbonProperties

object DataFrameAPIExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
        .setAppName("DataFrameAPIExample")
        .setMaster("local[2]")
    )
    sc.setLogLevel("WARN")
    textToCarbon(sc)
  }

  def textToCarbon(sc: SparkContext): Unit = {
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
        .getCanonicalPath
    val storePath = currentDirectory + "/target/store"
    val kettleHome = new File(currentDirectory + "/../processing/carbonplugins")
        .getCanonicalPath
    val hiveMetaPath = currentDirectory + "/target/hivemetadata"
    val cc = new CarbonContext(sc, storePath)
    cc.setConf("carbon.kettle.home", kettleHome)
    cc.setConf("hive.metastore.warehouse.dir", hiveMetaPath)
    CarbonProperties.getInstance().addProperty("carbon.table.split.partition.enable", "false")

    import cc.implicits._
    // create a dataframe
    val df = sc.parallelize(1 to 1000)
        .map(x => ("a", "b", x))
        .toDF("c1", "c2", "c3")

    // save dataframe to carbon file
    df.write
        .format("org.apache.spark.sql.CarbonSource")
        .option("tableName", "carbon1")
        .mode(SaveMode.Overwrite)
        .save()

    // use datasource api to read
    val in = cc.read
        .format("org.apache.spark.sql.CarbonSource")
        .option("tableName", "carbon1")
        .load()

    val count = in.where($"c3" > 500).select($"*").count()

    // scalastyle:off println
    println(s"count using dataframe.read: $count")
    // scalastyle:on println

    // use SQL to read
    cc.sql("select count(*) from carbon1 where c3 > 500").show
    cc.sql("drop cube carbon1")

    // also support a implicit function for easier access
    import org.carbondata.spark._
    df.saveAsCarbonFile(Map("tableName" -> "carbon2"))
    cc.sql("select count(*) from carbon2 where c3 > 100").show
    cc.sql("drop cube carbon2")
  }
}
