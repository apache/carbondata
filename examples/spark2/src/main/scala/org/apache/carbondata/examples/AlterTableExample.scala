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
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession


object AlterTableExample {

  def main(args: Array[String]): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("AlterTableExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS new_carbon_table")

    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField char(5),
         |    floatField float,
         |    complexData array<string>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    // Alter table change data type
    spark.sql("DESCRIBE FORMATTED carbon_table").show()
    spark.sql("ALTER TABLE carbon_table CHANGE intField intField bigint").show()

    // Alter table add columns
    spark.sql("DESCRIBE FORMATTED carbon_table").show()
    spark.sql("ALTER TABLE carbon_table ADD COLUMNS (newField String)" +
              " TBLPROPERTIES ('DEFAULT.VALUE.newField'='def')").show()

    // Alter table drop columns
    spark.sql("DESCRIBE FORMATTED carbon_table").show()
    spark.sql("ALTER TABLE carbon_table DROP COLUMNS (newField)").show()
    spark.sql("DESCRIBE FORMATTED carbon_table").show()

    // Alter table rename table name
    spark.sql("SHOW TABLES").show()
    spark.sql("ALTER TABLE carbon_table RENAME TO new_carbon_table").show()
    spark.sql("SHOW TABLES").show()

    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS new_carbon_table")

  }
}
