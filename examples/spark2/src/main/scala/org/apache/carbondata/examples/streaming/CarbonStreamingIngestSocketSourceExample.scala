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
import org.apache.spark.sql.streaming.ProcessingTime

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.utils.StreamingExampleUtil


/**
 * This example reads stream data from socket source (input) and write into
 * existing carbon table(output).
 *
 * It uses localhost and port (9999) to create a socket and write to it.
 * Exmaples uses two threads one to write data to socket and other thread
 * to receive data from socket and write into carbon table.
 */

// scalastyle:off println
object CarbonStreamingIngestSocketSourceExample {

  def main(args: Array[String]) {

    // setup localhost and port number
    val host = "localhost"
    val port = 9999
    // setup paths
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val csvDataDir = s"$rootPath/examples/spark2/resources/csvDataDir"
    val streamTableName = s"_carbon_socket_stream_table_"
    val streamTablePath = s"$storeLocation/default/$streamTableName"
    val ckptLocation = s"$rootPath/examples/spark2/resources/ckptDir"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // cleanup residual files, if any
    StreamingExampleUtil.cleanUpDir(csvDataDir, ckptLocation)

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CarbonNetworkStreamingExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("ERROR")

    // Writes Dataframe to CarbonData file:
    import spark.implicits._

    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")

    // Create target carbon table and populate with initial data
    spark.sql(
      s"""
         | CREATE TABLE ${streamTableName}(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT
         | )
         | STORED BY 'carbondata'""".stripMargin)

    // Generate CSV data and write to CSV file
    StreamingExampleUtil.generateCSVDataFile(spark, 1, csvDataDir, SaveMode.Overwrite)

    // load the table
    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$csvDataDir'
         | INTO TABLE ${streamTableName}
         | OPTIONS('FILEHEADER'='id,name,city,salary'
         | )""".stripMargin)


    spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Create server socket in main thread
    val serverSocket = StreamingExampleUtil.createserverSocket(host, port)

    // Start client thread to receive streaming data and write into carbon
    val streamWriterThread: Thread = new Thread() {
      override def run(): Unit= {

        try {
          // Setup read stream to read input data from socket
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", host)
            .option("port", port)
            .load()

          // Write data from socket stream to carbondata file
          val qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime("2 seconds"))
            .option("checkpointLocation", ckptLocation)
            .option("path", streamTablePath)
            .start()

          qry.awaitTermination()
        } catch {
          case e: InterruptedException => println("Done reading and writing streaming data")
        }
      }
    }
    streamWriterThread.start()

    // wait for client to connection request and accept
    val clientSocket = StreamingExampleUtil.waitToForClientConnection(serverSocket.get)

    // Write to client's connected socket every 2 seconds, for 5 times
    StreamingExampleUtil.writeToSocket(clientSocket, 5, 2, 11)

    Thread.sleep(2000)
    // interrupt client thread to stop streaming query
    streamWriterThread.interrupt()
    //wait for client thread to finish
    streamWriterThread.join()

    //Close the server socket
    serverSocket.get.close()

    // verify streaming data is added into the table
    // spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Cleanup residual files and table data
    StreamingExampleUtil.cleanUpDir(csvDataDir, ckptLocation)
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")
  }
}
// scalastyle:on println
