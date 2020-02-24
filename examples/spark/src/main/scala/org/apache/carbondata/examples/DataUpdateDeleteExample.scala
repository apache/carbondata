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
import org.apache.carbondata.examples.util.ExampleUtils

object DataUpdateDeleteExample {

  def main(args: Array[String]) {

    val spark = ExampleUtils.createSparkSession("DataUpdateDeleteExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    import spark.implicits._
    // Drop table
    spark.sql("DROP TABLE IF EXISTS IUD_table1")
    spark.sql("DROP TABLE IF EXISTS IUD_table2")

    // Simulate data and write to table IUD_table1
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    var df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (x, new java.sql.Date(sdf.parse("2015-07-" + (x % 10 + 10)).getTime),
        "china", "aaa" + x, "phone" + 555 * x, "ASD" + (60000 + x), 14999 + x))
      .toDF("IUD_table1_id", "IUD_table1_date", "IUD_table1_country", "IUD_table1_name",
        "IUD_table1_phonetype", "IUD_table1_serialname", "IUD_table1_salary")
    df.write
      .format("carbondata")
      .option("tableName", "IUD_table1")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()

    // Simulate data and write to table IUD_table2
    df = spark.sparkContext.parallelize(1 to 10)
      .map(x => (x, new java.sql.Date(sdf.parse("2017-07-" + (x % 20 + 1)).getTime),
        "usa", "bbb" + x, "phone" + 100 * x, "ASD" + (1000 * x - x), 25000 + x))
      .toDF("IUD_table2_id", "IUD_table2_date", "IUD_table2_country", "IUD_table2_name",
        "IUD_table2_phonetype", "IUD_table2_serialname", "IUD_table2_salary")
    df.write
      .format("carbondata")
      .option("tableName", "IUD_table2")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()
    spark.sql("""
           SELECT * FROM IUD_table2 ORDER BY IUD_table2_id
           """).show()

    // 1.Update data with simple SET
    // Update data where salary < 15003
    val dateStr = "2018-08-08"
    spark.sql(s"""
           UPDATE IUD_table1 SET (IUD_table1_date, IUD_table1_country) = ('$dateStr', 'india')
           WHERE IUD_table1_salary < 15003
           """).show()
    // Query data again after the above update
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()

    spark.sql("""
           UPDATE IUD_table1 SET (IUD_table1_salary) = (IUD_table1_salary + 9)
           WHERE IUD_table1_name = 'aaa1'
           """).show()
    // Query data again after the above update
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()

    // 2.Update data with subquery result SET
    spark.sql("""
         UPDATE IUD_table1
         SET (IUD_table1_country, IUD_table1_name) = (SELECT IUD_table2_country, IUD_table2_name
         FROM IUD_table2 WHERE IUD_table2_id = 5)
         WHERE IUD_table1_id < 5""").show()
    spark.sql("""
         UPDATE IUD_table1
         SET (IUD_table1_date, IUD_table1_serialname, IUD_table1_salary) =
         (SELECT '2099-09-09', IUD_table2_serialname, '9999'
         FROM IUD_table2 WHERE IUD_table2_id = 5)
         WHERE IUD_table1_id < 5""").show()

    // Query data again after the above update
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()

    // 3.Update data with join query result SET
    spark.sql("""
         UPDATE IUD_table1
         SET (IUD_table1_country, IUD_table1_salary) =
         (SELECT IUD_table2_country, IUD_table2_salary FROM IUD_table2 FULL JOIN IUD_table1 u
         WHERE u.IUD_table1_id = IUD_table2_id and IUD_table2_id=6)
         WHERE IUD_table1_id >6""").show()

    // Query data again after the above update
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()

    // 4.Delete data where salary > 15005
    spark.sql("""
           DELETE FROM IUD_table1 WHERE IUD_table1_salary > 15005
           """).show()

    // Query data again after delete data
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()

    // 5.Delete data WHERE id in (1, 2, $key)
    val key = 3
    spark.sql(s"""
           DELETE FROM IUD_table1 WHERE IUD_table1_id in (1, 2, $key)
           """).show()

    // Query data again after delete data
    spark.sql("""
           SELECT * FROM IUD_table1 ORDER BY IUD_table1_id
           """).show()

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    // Drop table
    spark.sql("DROP TABLE IF EXISTS IUD_table1")
    spark.sql("DROP TABLE IF EXISTS IUD_table2")
  }

}
