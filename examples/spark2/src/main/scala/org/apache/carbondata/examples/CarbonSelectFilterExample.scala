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

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.SparkSession


object CarbonSelectFilterExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty("carbon.kettle.home", s"$rootPath/processing/carbonplugins")
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS customer")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE customer(
         |    id String,
         |    name  String,
         |    orders long
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/customer.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE customer
         | options('FILEHEADER'='id,name,orders')
       """.stripMargin)
    // scalastyle:on

    spark.sql("""
             SELECT *
             FROM customer
             where id >= 1
              """).show()

    spark.sql("""
             SELECT *
             FROM customer
             where id >= 1 and id < 5
              """).show()

    spark.sql("""
             SELECT *
             FROM customer
             where id > 1 and id < 3
              """).show()

    spark.sql("""
             SELECT *
             FROM customer
             where id between 1 and 3
              """).show()

    spark.sql("DROP TABLE IF EXISTS customer")
  }

}
