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
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}

import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.examples.util.ExampleUtils

case class FileElement(school: Array[String], age: Int)
case class StreamData(id: Int, name: String, city: String, salary: Float, file: FileElement)

// scalastyle:off println
object StreamingWithRowParserExample {
  def main(args: Array[String]) {

    // setup paths
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val spark = ExampleUtils.createSparkSession("StreamingWithRowParserExample", 4)
    val streamTableName = s"stream_table_with_row_parser"

    val requireCreateTable = true
    val useComplexDataType = false

    if (requireCreateTable) {
      // drop table if exists previously
      spark.sql(s"DROP TABLE IF EXISTS ${ streamTableName }")
      // Create target carbon table and populate with initial data
      if (useComplexDataType) {
        spark.sql(
          s"""
             | CREATE TABLE ${ streamTableName }(
             | id INT,
             | name STRING,
             | city STRING,
             | salary FLOAT,
             | file struct<school:array<string>, age:int>
             | )
             | STORED AS carbondata
             | TBLPROPERTIES(
             | 'streaming'='true', 'sort_columns'='name')
             | """.stripMargin)
      } else {
        spark.sql(
          s"""
             | CREATE TABLE ${ streamTableName }(
             | id INT,
             | name STRING,
             | city STRING,
             | salary FLOAT
             | )
             | STORED AS carbondata
             | TBLPROPERTIES(
             | 'streaming'='true', 'sort_columns'='name')
             | """.stripMargin)
      }

      val carbonTable = CarbonEnv.getCarbonTable(Some("default"), streamTableName)(spark)
      // batch load
      val path = s"$rootPath/examples/spark/src/main/resources/streamSample.csv"
      spark.sql(
        s"""
           | LOAD DATA LOCAL INPATH '$path'
           | INTO TABLE $streamTableName
           | OPTIONS('HEADER'='true')
         """.stripMargin)

      // streaming ingest
      val serverSocket = new ServerSocket(7071)
      val thread1 = startStreaming(spark, carbonTable.getTablePath)
      val thread2 = writeSocket(serverSocket)
      val thread3 = showTableCount(spark, streamTableName)

      System.out.println("type enter to interrupt streaming")
      System.in.read()
      thread1.interrupt()
      thread2.interrupt()
      thread3.interrupt()
      serverSocket.close()
    }

    spark.sql(s"select count(*) from ${ streamTableName }").show(100, truncate = false)

    spark.sql(s"select * from ${ streamTableName }").show(100, truncate = false)

    // record(id = 100000001) comes from batch segment_0
    // record(id = 1) comes from stream segment_1
    spark.sql(s"select * " +
              s"from ${ streamTableName } " +
              s"where id = 100000001 or id = 1 limit 100").show(100, truncate = false)

    // not filter
    spark.sql(s"select * " +
              s"from ${ streamTableName } " +
              s"where id < 10 limit 100").show(100, truncate = false)

    if (useComplexDataType) {
      // complex
      spark.sql(s"select file.age, file.school " +
                s"from ${ streamTableName } " +
                s"where where file.age = 30 ").show(100, truncate = false)
    }

    spark.stop()
    System.out.println("streaming finished")
  }

  def showTableCount(spark: SparkSession, tableName: String): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        for (_ <- 0 to 1000) {
          spark.sql(s"select count(*) from $tableName").show(truncate = false)
          Thread.sleep(1000 * 3)
        }
      }
    }
    thread.start()
    thread
  }

  def startStreaming(spark: SparkSession, tablePath: String): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          import spark.implicits._
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 7071)
            .load()
            .as[String]
            .map(_.split(","))
            .map { fields => {
              val tmp = fields(4).split("\\$")
              val file = FileElement(tmp(0).split(":"), tmp(1).toInt)
              if (fields(0).toInt % 2 == 0) {
                StreamData(fields(0).toInt, null, fields(2), fields(3).toFloat, file)
              } else {
                StreamData(fields(0).toInt, fields(1), fields(2), fields(3).toFloat, file)
              }
            } }

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime("5 seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(tablePath))
            .option("dbName", "default")
            .option("tableName", "stream_table_with_row_parser")
            .start()

          qry.awaitTermination()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
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
        for (_ <- 1 to 1000) {
          // write 5 records per iteration
          for (_ <- 0 to 1000) {
            index = index + 1
            socketWriter.println(index.toString + ",name_" + index
                                 + ",city_" + index + "," + (index * 10000.00).toString +
                                 ",school_" + index + ":school_" + index + index + "$" + index)
          }
          socketWriter.flush()
          Thread.sleep(1000)
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
