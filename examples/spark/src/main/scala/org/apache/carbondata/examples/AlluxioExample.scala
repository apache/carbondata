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
import java.util.Date

import alluxio.cli.fs.FileSystemShell
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * configure alluxio:
 * 1.start alluxio
 * 2. Please upload data to alluxio if you set runShell as false
 * ./bin/alluxio fs copyFromLocal /carbondata_path/hadoop/src/test/resources/data.csv /
 * 3.Get more details at: https://www.alluxio.org/docs/1.8/en/compute/Spark.html
 */
object AlluxioExample {
  def main (args: Array[String]) {
    val carbon = ExampleUtils.createSparkSession("AlluxioExample")
    val runShell: Boolean = if (null != args && args.length > 0) {
      args(0).toBoolean
    } else {
      true
    }
    exampleBody(carbon, runShell)
    carbon.close()
  }

  def exampleBody (spark: SparkSession, runShell: Boolean = true): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    spark.sparkContext.hadoopConfiguration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
    FileFactory.getConfiguration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")

    // Specify date format based on raw data
    CarbonProperties.getInstance()
            .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    val time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
    val alluxioPath = "alluxio://localhost:19998"
    var alluxioFile = alluxioPath + "/data.csv"

    val remoteFile = "/carbon_alluxio" + time + ".csv"

    var mFsShell: FileSystemShell = null

    // avoid dependency alluxio shell when running it with spark-submit
    if (runShell) {
      mFsShell = new FileSystemShell()
      alluxioFile = alluxioPath + remoteFile
      val localFile = rootPath + "/hadoop/src/test/resources/data.csv"
      mFsShell.run("copyFromLocal", localFile, remoteFile)
    }

    import spark._

    sql("DROP TABLE IF EXISTS alluxio_table")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS alluxio_table(
         |    ID Int,
         |    date Date,
         |    country String,
         |    name String,
         |    phonetype String,
         |    serialname String,
         |    salary Int)
         | STORED AS carbondata
         | TBLPROPERTIES(
         |    'SORT_COLUMNS' = 'phonetype,name',
         |    'TABLE_BLOCKSIZE'='32',
         |    'AUTO_LOAD_MERGE'='true')
       """.stripMargin)

    for (i <- 0 until 2) {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$alluxioFile'
           | into table alluxio_table
         """.stripMargin)
    }

    sql("SELECT * FROM alluxio_table").show()

    sql("SHOW SEGMENTS FOR TABLE alluxio_table").show()
    sql(
      """
        | SELECT country, count(salary) AS amount
        | FROM alluxio_table
        | WHERE country IN ('china','france')
        | GROUP BY country
      """.stripMargin).show()

    for (i <- 0 until 2) {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$alluxioFile'
           | into table alluxio_table
         """.stripMargin)
    }
    sql("SHOW SEGMENTS FOR TABLE alluxio_table").show()
    if (runShell) {
      mFsShell.run("rm", remoteFile)
      mFsShell.close()
    }
    sql("DROP TABLE IF EXISTS alluxio_table")
  }
}
