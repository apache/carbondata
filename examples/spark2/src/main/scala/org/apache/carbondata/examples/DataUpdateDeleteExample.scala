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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object DataUpdateDeleteExample {

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath

    var hdfsStoreFlg = false;
    if (args != null && args.size > 0) {
      if ("true".equalsIgnoreCase(args(0))) {
        hdfsStoreFlg = true
      }
    }
    var storeLocation = s"$rootPath/examples/spark2/target/store"
    var warehouse = s"$rootPath/examples/spark2/target/warehouse"
    var metastoredb = s"$rootPath/examples/spark2/target"
    var testData = s"$rootPath/examples/spark2/src/main/resources/data_update.csv"
    if (hdfsStoreFlg) {
      storeLocation = "hdfs://nameservice1/carbon2/data/"
      warehouse = "hdfs://nameservice1/carbon2/warehouse/"
      metastoredb = "hdfs://nameservice1/carbon2/carbonstore/"
      testData = "hdfs://nameservice1/carbon2/data_update.csv"
    }

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreateCarbonSession(storeLocation, metastoredb)
    spark.sparkContext.setLogLevel("WARN")

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    import spark.implicits._
    // Drop table
    spark.sql("DROP TABLE IF EXISTS update_table")
    spark.sql("DROP TABLE IF EXISTS big_table")

    spark.sql(s"""
           CREATE TABLE IF NOT EXISTS update_table
           (ID Int, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    spark.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' INTO TABLE update_table
           """)

    val df = spark.sparkContext.parallelize(1 to 2000000)
      .map(x => (x, "name" + (1000000 + x), "2017/07/" + (x % 20 + 1),
        "china", 2 * x))
      .toDF("id", "name", "date", "country", "salary")
    df.write
      .format("carbondata")
      .option("tableName", "big_table")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()

    // loop update and delete in big_table
    var loopCnt = 5
    for (index <- 1 to loopCnt) {
      // Update country with simple SET
      var name = "name" + (1200000 + index)
      spark.sql(s"""
           UPDATE big_table SET (country) = ('india') WHERE name = '$name'
           """).show()
      // Query data after the above update
      spark.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // Update date with simple SET
      name = "name" + (1200000 + loopCnt + index)
      spark.sql(s"""
           UPDATE big_table SET (date) = ('2018/08/08') WHERE name = '$name'
           """).show()
      // Query data after the above update
      spark.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // Update salary with simple SET
      name = "name" + (1200000 + loopCnt * 2 + index)
      spark.sql(s"""
           UPDATE big_table SET (salary) = (9999999) WHERE name = '$name'
           """).show()
      // Query data after the above update
      spark.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // Update data with subquery result SET
      var id = loopCnt + index
      spark.sql(s"""
         UPDATE big_table
         SET (big_table.country, big_table.name) = (SELECT u.country,
          u.name FROM update_table u WHERE u.id = $index)
         WHERE big_table.id < $id""").show()
      // Query data after the above update
      spark.sql(s"""
           SELECT * FROM big_table where big_table.id < $id
           """).show()

      // Update data with join query result SET
      id = 2000000 - index * 10
      val id1 = loopCnt * 3 + index
      val id2 = loopCnt * 4 + index
      spark.sql(s"""
         UPDATE big_table
         SET (big_table.country, big_table.salary) =
         (SELECT u.country, f.salary FROM update_table u FULL JOIN update_table f
         WHERE u.id = $id1  and f.id=$id2) WHERE big_table.id > $id""").show()
      // Query data after the above update
      spark.sql(s"""
           SELECT * FROM big_table where big_table.id < $id
           """).show()

      // delete by name
      name = "name" + (1200000 + loopCnt * 3 + index)
      spark.sql(s"""
           DELETE FROM big_table WHERE name = '$name'
           """).show()
      // Query data after the above delete
      spark.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // delete by salary
      var salary = 2000000*2 - index * 4
      spark.sql(s"""
           DELETE FROM big_table WHERE salary > $salary and salary < 9999999
           """).show()
      spark.sql(s"""
           SELECT * FROM big_table WHERE salary > $salary and salary < 9999999
           """).show()
    }

    spark.sql(s"""
           SELECT count(*) FROM big_table
           """).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS update_table")
    spark.sql("DROP TABLE IF EXISTS big_table")
  }

}
