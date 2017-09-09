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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.utils.StreamingCleanupUtil
import org.apache.commons.lang.RandomStringUtils

/**
  * Write data received from the network into carbondata file.
  *
  * Usage: CarbonNetworkStreamingExample <hostname> <port>
  * <hostname> and <port> describe the TCP server that Structured Streaming
  * would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9876`
  * and then run the example
  *    `$ bin/run-example sql.streaming.CarbondataNetworkStreamingExample
  *    localhost 9876`
  */
object CarbonDataNetworkStreamingExample {

  def main(args: Array[String]) {

    // get host and port number
    val host = args(0)
    val port = args(1).toInt

    if (args.length < 2) {
      System.err.println("Usage: CarbonNetworkStreamingIngestion <hostname> <port>")
      System.exit(1)
    }

    //setup paths
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val streamTableName = s"_carbon_socket_stream_table_"
    val streamTablePath = s"$storeLocation/default/$streamTableName"
    val ckptLocation = s"$rootPath/examples/spark2/resources/ckptDir"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    //cleanup any residual files
    StreamingCleanupUtil.main(Array(ckptLocation))

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonNetworkStreamingExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("ERROR")

    // Writes Dataframe to CarbonData file:
    import spark.implicits._

    //Generate random data
    val dataDF = spark.sparkContext.parallelize(1 to 10)
      .map(id => (id, "name_ABC", "city_XYZ", 10000*id)).
      toDF("id", "name", "city", "salary")

    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")

    // Create Carbon Table
    // Saves dataframe to carbondata file
    dataDF.write
      .format("carbondata")
      .option("tableName", streamTableName)
      .option("compress", "true")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()

    spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Create DataFrame representing the stream of input lines from connection to host:port
    val readSocketDF = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    //Write from socket stream to carbondata file
    val qry = readSocketDF.writeStream
      .format("carbondata")
      .option("checkpointLocation", ckptLocation)
      .option("path", streamTablePath)
      .start()

    // stop streaming query after 5 sec delay
    //Thread.sleep(5000)
    qry.awaitTermination()

    // verify streaming data is added into the table
    spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Cleanup residual files and table data
    StreamingCleanupUtil.main(Array(ckptLocation))
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")
  }
}
