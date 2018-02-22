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
package org.apache.carbondata.datamap.examples

import java.io.File

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.CarbonProperties

object MinMaxDataMapExample {
  def main(args: Array[String]): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "").getCanonicalPath
    val storeLocation = s"$rootPath/dataMap/examples/target/store"
    val warehouse = s"$rootPath/datamap/examples/target/warehouse"
    val metastoredb = s"$rootPath/datamap/examples/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonDataMapExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation)

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // register datamap writer
    DataMapStoreManager.getInstance().createAndRegisterDataMap(
      AbsoluteTableIdentifier.from(storeLocation, "default", "carbonminmax"),
      classOf[MinMaxIndexDataMapFactory].getName,
      MinMaxIndexDataMap.NAME)

    spark.sql("DROP TABLE IF EXISTS carbonminmax")

    val df = spark.sparkContext.parallelize(1 to 33000)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "c3")

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbonminmax")
      .mode(SaveMode.Overwrite)
      .save()

    // Query the table.
    spark.sql("select c2 from carbonminmax").show(20, false)
    spark.sql("select c2 from carbonminmax where c2 = 'b'").show(20, false)
    spark.sql("DROP TABLE IF EXISTS carbonminmax")

  }
}