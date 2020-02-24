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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.CarbonSparkStreamingFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.streaming.CarbonSparkStreamingListener

/**
 * This example introduces how to use Spark Streaming to write data
 * to CarbonData stream table.
 *
 * NOTE: Current integration with Spark Streaming is an alpha feature.
 */
// scalastyle:off println
object SparkStreamingExample {

  def main(args: Array[String]): Unit = {

    // setup paths
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val checkpointPath =
      s"$rootPath/examples/spark/target/spark_streaming_cp_" +
      System.currentTimeMillis().toString()
    val streamTableName = s"dstream_stream_table"

    val spark = ExampleUtils.createSparkSession("SparkStreamingExample", 4)

    val requireCreateTable = true

    if (requireCreateTable) {
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
           | STORED AS carbondata
           | TBLPROPERTIES(
           | 'streaming'='true',
           | 'sort_columns'='name')
           | """.stripMargin)
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
      val thread1 = writeSocket(serverSocket)
      val thread2 = showTableCount(spark, streamTableName)
      val ssc = startStreaming(spark, streamTableName, checkpointPath)
      // add a Spark Streaming Listener to remove all lock for stream tables when stop app
      ssc.sparkContext.addSparkListener(new CarbonSparkStreamingListener())
      // wait for stop signal to stop Spark Streaming App
      waitForStopSignal(ssc)
      // it need to start Spark Streaming App in main thread
      // otherwise it will encounter an not-serializable exception.
      ssc.start()
      ssc.awaitTermination()
      thread1.interrupt()
      thread2.interrupt()
      serverSocket.close()
    }

    spark.sql(s"select count(*) from ${ streamTableName }").show(100, truncate = false)

    spark.sql(s"select * from ${ streamTableName } order by id desc").show(100, truncate = false)

    // record(id = 100000001) comes from batch segment_0
    // record(id = 1) comes from stream segment_1
    spark.sql(s"select * " +
              s"from ${ streamTableName } " +
              s"where id = 100000001 or id = 1 limit 100").show(100, truncate = false)

    // not filter
    spark.sql(s"select * " +
              s"from ${ streamTableName } " +
              s"where id < 10 limit 100").show(100, truncate = false)

    // show segments
    spark.sql(s"SHOW SEGMENTS FOR TABLE ${streamTableName}").show(false)

    spark.stop()
    System.out.println("streaming finished")
  }

  def showTableCount(spark: SparkSession, tableName: String): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        for (_ <- 0 to 1000) {
          println(System.currentTimeMillis())
          spark.sql(s"select count(*) from $tableName").show(truncate = false)
          spark.sql(s"SHOW SEGMENTS FOR TABLE ${tableName}").show(false)
          Thread.sleep(1000 * 5)
        }
      }
    }
    thread.start()
    thread
  }

  def waitForStopSignal(ssc: StreamingContext): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        // use command 'nc 127.0.0.1 7072' to stop Spark Streaming App
        new ServerSocket(7072).accept()
        // don't stop SparkContext here
        ssc.stop(false, true)
      }
    }
    thread.start()
    thread
  }

  def startStreaming(spark: SparkSession, tableName: String,
      checkpointPath: String): StreamingContext = {
    var ssc: StreamingContext = null
    try {
      // recommend: the batch interval must set larger, such as 30s, 1min.
      ssc = new StreamingContext(spark.sparkContext, Seconds(30))
      ssc.checkpoint(checkpointPath)

      val readSocketDF = ssc.socketTextStream("localhost", 7071)

      val batchData = readSocketDF
        .map(_.split(","))
        .map(fields => DStreamData(fields(0).toInt, fields(1), fields(2), fields(3).toFloat))

      println("init carbon table info")
      batchData.foreachRDD { (rdd: RDD[DStreamData], time: Time) => {
        val df = spark.createDataFrame(rdd).toDF()
        println(System.currentTimeMillis().toString() +
          " at batch time: " + time.toString() +
          " the count of received data: " + df.count())
        CarbonSparkStreamingFactory.getStreamSparkStreamingWriter(spark, "default", tableName)
          .mode(SaveMode.Append)
          .writeStreamData(df, time)
      }}
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        println("Done reading and writing streaming data")
    }
    ssc
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
          for (_ <- 0 to 100) {
            index = index + 1
            socketWriter.println(index.toString + ",name_" + index
                                 + ",city_" + index + "," + (index * 10000.00).toString +
                                 ",school_" + index + ":school_" + index + index + "$" + index)
          }
          socketWriter.flush()
          Thread.sleep(2000)
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
