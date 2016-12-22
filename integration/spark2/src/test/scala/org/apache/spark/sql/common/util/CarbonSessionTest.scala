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

package org.apache.spark.sql.common.util

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.commons.io.FileUtils

object CarbonSessionTest extends{

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    val spark = {

        // clean data folder
        if (true) {
            val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
            clean(storeLocation)
            clean(warehouse)
            clean(metastoredb)
        }

        val spark = SparkSession
          .builder()
          .master("local")
          .appName("CarbonExample")
          .enableHiveSupport()
          .config("spark.sql.warehouse.dir", warehouse)
          .config("javax.jdo.option.ConnectionURL",
              s"jdbc:derby:;databaseName=$metastoredb;create=true")
          .getOrCreate()

        CarbonProperties.getInstance()
          .addProperty("carbon.kettle.home", s"$rootPath/processing/carbonplugins")
          .addProperty("carbon.storelocation", storeLocation)

        spark.sparkContext.setLogLevel("WARN")

        spark
    }

    val sc = spark.sparkContext

    lazy val implicits = spark.implicits

    def sql(sqlText: String): DataFrame  = spark.sql(sqlText)

}


