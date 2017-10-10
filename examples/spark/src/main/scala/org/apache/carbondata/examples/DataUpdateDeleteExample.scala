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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object DataUpdateDeleteExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("DataUpdateDeleteExample")

    // for local files
    var rootPath = ExampleUtils.currentPath
    // for hdfs files
    // var rootPath = "hdfs://hdfs-host/carbon"

    val testData = rootPath + "/src/main/resources/data.csv"

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    cc.sql("DROP TABLE IF EXISTS t3")
    cc.sql("DROP TABLE IF EXISTS t5")

    // Create table, 6 dimensions, 1 measure
    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (id Int, date Date, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' INTO TABLE t3
           """)

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    // Simulate data and write to table t5
    var fields = Seq[StructField]()
    fields = fields :+ DataTypes.createStructField("t5_id", DataTypes.IntegerType, false)
    fields = fields :+ DataTypes.createStructField("t5_date", DataTypes.DateType, false)
    fields = fields :+ DataTypes.createStructField("t5_country", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("t5_name", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("t5_phonetype", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("t5_serialname", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("t5_salary", DataTypes.IntegerType, false)
    var schema = StructType(fields)
    var sdf = new SimpleDateFormat("yyyy-MM-dd")
    var data = cc.sparkContext.parallelize(1 to 10).map { x =>
      val day = x % 20 + 1
      var dateStr = ""
      if (day >= 10) {
        dateStr = "2017-07-" + day
      } else {
        dateStr = "2017-07-0" + day
      }
      val dt = new java.sql.Date(sdf.parse(dateStr).getTime);
      var row = Seq[Any]()
      row = row :+ x
      row = row :+ dt
      row = row :+ "china"
      row = row :+ "bbb" + x
      row = row :+ "phone" + 100 * x
      row = row :+ "ASD" + (1000 * x - x)
      row = row :+ (25000 + x)
      Row.fromSeq(row)
    }
    var df = cc.createDataFrame(data, schema)
    df.write
      .format("carbondata")
      .option("tableName", "t5")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
    cc.sql("""
           SELECT * FROM t5 ORDER BY t5_id
           """).show()

    // 1.Update data with simple SET
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    // Update data where salary < 15003
    val dateStr = "2018-08-08"
    cc.sql(s"""
           UPDATE t3 SET (t3.date, t3.country) = ('$dateStr', 'india') WHERE t3.salary < 15003
           """).show()
    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    cc.sql("""
           UPDATE t3 SET (t3.salary) = (t3.salary + 9) WHERE t3.name = 'aaa1'
           """).show()
    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    // 2.Update data with subquery result SET
    cc.sql("""
         UPDATE t3
         SET (t3.country, t3.name) = (SELECT t5_country, t5_name FROM t5 WHERE t5_id = 5)
         WHERE t3.id < 5""").show()
    cc.sql("""
         UPDATE t3
         SET (t3.date, t3.serialname, t3.salary) =
         (SELECT '2099-09-09', t5_serialname, '9999' FROM t5  WHERE t5_id = 5)
         WHERE t3.id < 5""").show()

    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    // 3.Update data with join query result SET
    cc.sql("""
         UPDATE t3
         SET (t3.country, t3.salary) =
         (SELECT t5_country, t5_salary FROM t5 FULL JOIN t3 u
         WHERE u.id = t5_id and t5_id=6) WHERE t3.id >6""").show()

    // Query data again after the above update
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    // 4.Delete data where salary > 15005
    cc.sql("""
           DELETE FROM t3 WHERE t3.salary > 15005
           """).show()

    // Query data again after delete data
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    // 5.Delete data WHERE id in (1, 2, $key)
    var key = 3
    cc.sql(s"""
           DELETE FROM t3 WHERE t3.id in (1, 2, $key)
           """).show()

    // Query data again after delete data
    cc.sql("""
           SELECT * FROM t3 ORDER BY t3.id
           """).show()

    // Drop table
    cc.sql("DROP TABLE IF EXISTS t3")
    cc.sql("DROP TABLE IF EXISTS t5")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

}
