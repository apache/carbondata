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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object DataUpdateDeleteExample {

  def main(args: Array[String]) {
    var hdfsStoreFlg = false;
    if (args != null && args.size > 0) {
      if ("true".equalsIgnoreCase(args(0))) {
        hdfsStoreFlg = true
      }
    }
    def currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath
    var storeLocation = currentPath + "/target/store"
    var metastoredb = currentPath + "/target/carbonmetastore"
    var testData = currentPath + "/src/main/resources/data.csv"
    if (hdfsStoreFlg) {
      storeLocation = "hdfs://nameservice1/carbon/store/"
      metastoredb = "hdfs://nameservice1/carbon2/carbonmetastore/"
      testData = "hdfs://nameservice1/carbon/data_update.csv"
    }

    val sc = new SparkContext(new SparkConf()
      .setAppName("DataUpdateDeleteExample")
      .setMaster("local[2]"))
    sc.setLogLevel("ERROR")
    // scalastyle:off println
    println(s"Starting DataUpdateDeleteExample using spark version ${sc.version}")
    // scalastyle:on println

    val cc = new CarbonContext(sc, storeLocation, metastoredb)
    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    // Drop table
    cc.sql("DROP TABLE IF EXISTS update_table")
    cc.sql("DROP TABLE IF EXISTS big_table")

    cc.sql(s"""
           CREATE TABLE IF NOT EXISTS update_table
           (ID Int, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' INTO TABLE update_table
           """)
    var fields = Seq[StructField]()
    fields = fields :+ DataTypes.createStructField("id", DataTypes.IntegerType, false)
    fields = fields :+ DataTypes.createStructField("name", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("date", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("country", DataTypes.StringType, false)
    fields = fields :+ DataTypes.createStructField("salary", DataTypes.IntegerType, false)
    val schema = StructType(fields)
    val data = cc.sparkContext.parallelize(1 to 2000000).map { x =>
      var row = Seq[Any]()
      row = row :+ x
      row = row :+ "name" + (1000000 + x)
      row = row :+ "2017/07/" + (x % 20 + 1)
      row = row :+ "china"
      row = row :+ 2 * x
      Row.fromSeq(row)
    }

    val df = cc.createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).parquet(PerfTest.savePath("temp"))
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
      cc.sql(s"""
           UPDATE big_table SET (country) = ('india') WHERE name = '$name'
           """).show()
      // Query data after the above update
      cc.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // Update date with simple SET
      name = "name" + (1200000 + loopCnt + index)
      cc.sql(s"""
           UPDATE big_table SET (date) = ('2018/08/08') WHERE name = '$name'
           """).show()
      // Query data after the above update
      cc.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // Update salary with simple SET
      name = "name" + (1200000 + loopCnt * 2 + index)
      cc.sql(s"""
           UPDATE big_table SET (salary) = (9999999) WHERE name = '$name'
           """).show()
      // Query data after the above update
      cc.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // Update data with subquery result SET
      var id = loopCnt + index
      cc.sql(s"""
         UPDATE big_table
         SET (big_table.country, big_table.name) = (SELECT u.country,
          u.name FROM update_table u WHERE u.id = $index)
         WHERE big_table.id < $id""").show()
      // Query data after the above update
      cc.sql(s"""
           SELECT * FROM big_table where big_table.id < $id
           """).show()

      // Update data with join query result SET
      id = 2000000 - index * 10
      val id1 = loopCnt * 3 + index
      val id2 = loopCnt * 4 + index
      cc.sql(s"""
         UPDATE big_table
         SET (big_table.country, big_table.salary) =
         (SELECT u.country, f.salary FROM update_table u FULL JOIN update_table f
         WHERE u.id = $id1  and f.id=$id2) WHERE big_table.id > $id""").show()
      // Query data after the above update
      cc.sql(s"""
           SELECT * FROM big_table where big_table.id < $id
           """).show()

      // delete by name
      name = "name" + (1200000 + loopCnt * 3 + index)
      cc.sql(s"""
           DELETE FROM big_table WHERE name = '$name'
           """).show()
      // Query data after the above delete
      cc.sql(s"""
           SELECT * FROM big_table WHERE name = '$name'
           """).show()

      // delete by salary
      var salary = 2000000*2 - index * 4
      cc.sql(s"""
           DELETE FROM big_table WHERE salary > $salary and salary < 9999999
           """).show()
      cc.sql(s"""
           SELECT * FROM big_table WHERE salary > $salary and salary < 9999999
           """).show()
    }

    cc.sql(s"""
           SELECT count(*) FROM big_table
           """).show()

    // Drop table
    cc.sql("DROP TABLE IF EXISTS update_table")
    cc.sql("DROP TABLE IF EXISTS big_table")
  }
}
