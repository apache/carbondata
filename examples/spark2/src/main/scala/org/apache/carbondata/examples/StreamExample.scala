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

import java.io.{File, PrintWriter}
import java.net.ServerSocket

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}

// scalastyle:off println
object StreamExample {
  def main(args: Array[String]) {

    // setup paths
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val streamTableName = s"stream_table"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StreamExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${ streamTableName }")
    // Create target carbon table and populate with initial data
    spark.sql(
      s"""
         | CREATE TABLE ${ streamTableName }(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT
         | )
         | STORED BY 'carbondata'""".stripMargin)

    val carbonTable = CarbonEnv.getInstance(spark).carbonMetastore.
      lookupRelation(Some("default"), streamTableName)(spark).asInstanceOf[CarbonRelation].
      tableMeta.carbonTable
    val tablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)

    val serverSocket = new ServerSocket(7071)
    var thread1 = startStreaming(spark, tablePath)
    val thread2 = writeSocket(serverSocket)
    val thread3 = showFileSize(tablePath.getPath)

    System.out.println("type enter to interrupt streaming")
    System.in.read()
    thread1.interrupt()

    // stop
    System.out.println("type enter to stop all")
    System.in.read()
    thread1.interrupt()
    thread2.interrupt()
    thread3.interrupt()
    serverSocket.close()
    spark.stop()
    System.out.println("streaming finished")
  }

  def showFileSize(path: String): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        for (i <- 0 to 100) {
          System.out.println("Table Size(byte): " + FileFactory.getDirectorySize(path))
          Thread.sleep(1000 * 3)
        }
      }
    }
    thread.start()
    return thread
  }

  def startStreaming(spark: SparkSession, tablePath: CarbonTablePath): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 7071)
            .load()

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime("5 seconds"))
            .option("checkpointLocation", tablePath.getStreamingCheckpointDir)
            .option("tablePath", tablePath.getPath)
            .start()

          qry.awaitTermination()
        } catch {
          case e: InterruptedException =>
            println("Done reading and writing streaming data")
        } finally {
          qry.stop()
        }
      }
    }
    thread.start()
    thread
  }

  def writeSocket(serverSocket: ServerSocket): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        // wait for client to connection request and accept
        val clientSocket = serverSocket.accept()
        val socketWriter = new PrintWriter(clientSocket.getOutputStream())
        var index = 0
        for (i <- 1 to 1000) {
          // write 5 records per iteration
          for (row <- 0 to 10000) {
            index = index + 1
            socketWriter.println(index.toString + ", name_" + index
                                 + ", city_" + index + ", " + (index * 10000.00).toString)
          }
          socketWriter.flush()
          Thread.sleep(5000)
        }
        socketWriter.close()
        System.out.println("Socket closed")
      }
    }
    thread.start()
    thread
  }
}
// scalastyle:on println
