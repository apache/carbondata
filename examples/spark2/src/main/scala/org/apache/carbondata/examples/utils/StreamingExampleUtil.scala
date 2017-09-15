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

package org.apache.carbondata.examples.utils

import java.io.{IOException, PrintWriter}
import java.net.{ServerSocket, Socket}

import scala.tools.nsc.io.Path

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}



/**
 * Utility functions for streaming ingest examples
 */

// scalastyle:off println
object StreamingExampleUtil {

  // Clean up directories recursively, accepts variable arguments
  def cleanUpDir(dirPaths: String*): Unit = {

      // if (args.length < 1) {
    if (dirPaths.size < 1) {
      System.err.println("Usage: StreamingCleanupUtil <dirPath> [dirpath]...")
      System.exit(1)
    }

    var i = 0
    while (i < dirPaths.size) {
      try {
        val path: Path = Path(dirPaths(i))
        path.deleteRecursively()
      } catch {
        case ioe: IOException => println("IO Exception while deleting files recursively" + ioe)
      }
      i = i + 1
    }
  }

  // Generates csv data and write to csv files at given path
  def generateCSVDataFile(spark: SparkSession,
                        idStart: Int,
                        csvDirPath: String,
                        saveMode: SaveMode): Unit = {
    // Create csv data frame file
    val csvRDD = spark.sparkContext.parallelize(1 to 10)
      .map(id => (id, "name_ABC", "city_XYZ", 10000.00*id))
      val csvDataDF = spark.createDataFrame(csvRDD).toDF("id", "name", "city", "salary")


    csvDataDF.write
      .option("header", "false")
      .mode(saveMode)
      .csv(csvDirPath)
  }

  // Generates csv data frame and returns to caller
  def generateCSVDataDF(spark: SparkSession,
                        idStart: Int): DataFrame = {
    // Create csv data frame file
    val csvRDD = spark.sparkContext.parallelize(1 to 10)
      .map(id => (id, "name_ABC", "city_XYZ", 10000.00*id))
    val csvDataDF = spark.createDataFrame(csvRDD).toDF("id", "name", "city", "salary")
    csvDataDF
  }

  // Create server socket for socket streaming source
  def createserverSocket(host: String, port: Int): Option[ServerSocket] = {
    try {
      Some(new ServerSocket(port))
    } catch {
      case e: java.net.ConnectException =>
        println("Error Connecting to" + host + ":" + port, e)
        None
    }
  }

  // Create server socket for socket streaming source
  def waitToForClientConnection(serverSocket: ServerSocket): Socket = {
    serverSocket.accept()
  }

  // Create server socket for socket streaming source
  def closeServerSocket(serverSocket: ServerSocket): Unit = {
    serverSocket.close()
  }

  // write periodically on given socket
  def writeToSocket(clientSocket: Socket,
                   iterations: Int,
                   delay: Int,
                   startID: Int): Unit = {

    var nItr = 10
    var nDelay = 5

    // iterations range check
    if (iterations >= 1 || iterations <= 50) {
      nItr = iterations
    } else {
      println("Number of iterations exceeds limit. Setting to default 10 iterations")
    }

    // delay range check (1 second to 60 seconds)
    if (delay >= 1 || delay <= 60) {
      nDelay = delay
    } else {
      println("Delay exceeds the limit. Setting it to default 2 seconds")
    }

    val socketWriter = new PrintWriter(clientSocket.getOutputStream())

    var j = startID

    for (i <- startID to startID + nItr) {
      // write 5 records per iteration
      for (id <- j to j + 5 ) {
        socketWriter.println(id.toString + ", name_" + i
          + ", city_" + i + ", " + (i*10000.00).toString)
      }
      j = j + 5
      socketWriter.flush()
      Thread.sleep(nDelay*1000)
    }
    socketWriter.close()
  }
}
// scalastyle:on println
