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
import org.apache.spark.sql.CarbonContext

/**
 * example for global dictionary generation
 * pls check directory of target/store/default and verify global dict file
 */
object GenerateDictionaryExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf()
      .setAppName("GenerateDictionaryExample")
      .setMaster("local[2]"))

    val pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
      .replace('\\', '/')
    val storeLocation = pwd + "/target/store"
    val factFilePath = pwd + "/src/main/resources/factSample.csv"
    val dimTableFilePath = pwd + "/src/main/resources/dimSample.csv"
    val kettleHome = new File(pwd + "/../processing/carbonplugins").getCanonicalPath
    val hiveMetaPath = pwd + "/target/hivemetadata"
    val hiveMetaStoreDB = pwd + "/target/metastore_db"

    val cc = new CarbonContext(sc, storeLocation)
    cc.setConf("carbon.kettle.home", kettleHome)
    cc.setConf("hive.metastore.warehouse.dir", hiveMetaPath)
    cc.setConf("javax.jdo.option.ConnectionURL", s"jdbc:derby:;" +
      s"databaseName=$hiveMetaStoreDB;create=true")

    // When you execute the second time, need to enable it
    // cc.sql("DROP CUBE dictSample")
    cc.sql("CREATE CUBE dictSample DIMENSIONS (id INTEGER, name STRING, city STRING) " +
      "MEASURES (salary INTEGER) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    cc.sql(s"LOAD DATA FACT FROM '$factFilePath' INTO CUBE dictSample " +
      s"OPTIONS(DELIMITER ',', FILEHEADER '')")
  }

}
