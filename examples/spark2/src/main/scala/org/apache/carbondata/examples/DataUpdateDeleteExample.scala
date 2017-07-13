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
import java.text.SimpleDateFormat

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object DataUpdateDeleteExample {

  def main(args: Array[String]) {

    // for local files
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    // for hdfs files
    // var rootPath = "hdfs://hdfs-host/carbon"

    var storeLocation = s"$rootPath/examples/spark2/target/store"
    var warehouse = s"$rootPath/examples/spark2/target/warehouse"
    var metastoredb = s"$rootPath/examples/spark2/target"

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("DataUpdateDeleteExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreateCarbonSession(storeLocation, metastoredb)
    spark.sparkContext.setLogLevel("WARN")

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    import spark.implicits._
    // Drop table
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t5")

     // Simulate data and write to table t3
    var sdf = new SimpleDateFormat("yyyy-MM-dd")
    var df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (x, new java.sql.Date(sdf.parse("2015-07-" + (x % 10 + 10)).getTime),
        "china", "aaa" + x, "phone" + 555 * x, "ASD" + (60000 + x), 14999 + x))
      .toDF("t3_id", "t3_date", "t3_country", "t3_name", "t3_phonetype", "t3_serialname", "t3_salary")
    df.write
      .format("carbondata")
      .option("tableName", "t3")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()

    // Simulate data and write to table t5
    df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (x, new java.sql.Date(sdf.parse("2017-07-" + (x % 20 + 1)).getTime),
        "usa", "bbb" + x, "phone" + 100 * x, "ASD" + (1000 * x - x), 25000 + x))
      .toDF("t5_id", "t5_date", "t5_country", "t5_name", "t5_phonetype", "t5_serialname", "t5_salary")
    df.write
      .format("carbondata")
      .option("tableName", "t5")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()
    spark.sql("""
           SELECT * FROM t5 ORDER BY t5_id
           """).show()

    // 1.Update data with simple SET
    // Update data where salary < 15003
    val dateStr = "2018-08-08"
    spark.sql(s"""
           UPDATE t3 SET (t3_date, t3_country) = ('$dateStr', 'india') WHERE t3_salary < 15003
           """).show()
    // Query data again after the above update
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()

    spark.sql("""
           UPDATE t3 SET (t3_salary) = (t3_salary + 9) WHERE t3_name = 'aaa1'
           """).show()
    // Query data again after the above update
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()

    // 2.Update data with subquery result SET
    spark.sql("""
         UPDATE t3
         SET (t3_country, t3_name) = (SELECT t5_country, t5_name FROM t5 WHERE t5_id = 5)
         WHERE t3_id < 5""").show()
    spark.sql("""
         UPDATE t3
         SET (t3_date, t3_serialname, t3_salary) =
         (SELECT '2099-09-09', t5_serialname, '9999' FROM t5 WHERE t5_id = 5)
         WHERE t3_id < 5""").show()

    // Query data again after the above update
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()

    // 3.Update data with join query result SET
    spark.sql("""
         UPDATE t3
         SET (t3_country, t3_salary) =
         (SELECT t5_country, t5_salary FROM t5 FULL JOIN t3 u
         WHERE u.t3_id = t5_id and t5_id=6) WHERE t3_id >6""").show()

    // Query data again after the above update
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()

    // 4.Delete data where salary > 15005
    spark.sql("""
           DELETE FROM t3 WHERE t3_salary > 15005
           """).show()

    // Query data again after delete data
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()

    // 5.Delete data WHERE id in (1, 2, $key)
    var key = 3
    spark.sql(s"""
           DELETE FROM t3 WHERE t3_id in (1, 2, $key)
           """).show()

    // Query data again after delete data
    spark.sql("""
           SELECT * FROM t3 ORDER BY t3_id
           """).show()

    // Drop table
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t5")

    spark.stop()
  }

}
