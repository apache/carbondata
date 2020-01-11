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

import java.net.ServerSocket

import org.apache.carbondata.examples.util.ExampleUtils

// scalastyle:off println
object StreamSQLExample {
  def main(args: Array[String]) {

    val spark = ExampleUtils.createSparkSession("StructuredStreamingExample", 4)
    val requireCreateTable = true
    val recordFormat = "json" // can be "json" or "csv"

    if (requireCreateTable) {
      // drop table if exists previously
      spark.sql(s"DROP TABLE IF EXISTS sink")
      spark.sql("DROP TABLE IF EXISTS source")

      // Create target carbon table and populate with initial data
      spark.sql(
        s"""
           | CREATE TABLE sink(
           | id INT,
           | name STRING,
           | salary FLOAT,
           | file struct<school:array<string>, age:int>
           | )
           | STORED AS carbondata
           | TBLPROPERTIES(
           | 'streaming'='true', 'sort_columns'='')
          """.stripMargin)
    }

    spark.sql(
      s"""
        | CREATE TABLE source (
        | id INT,
        | name STRING,
        | salary FLOAT,
        | file struct<school:array<string>, age:int>
        | )
        | STORED AS carbondata
        | TBLPROPERTIES(
        | 'streaming'='source',
        | 'format'='socket',
        | 'host'='localhost',
        | 'port'='7071',
        | 'record_format'='$recordFormat'
        | )
      """.stripMargin)

    val serverSocket = new ServerSocket(7071)

    // start ingest streaming job
    spark.sql(
      s"""
        | CREATE STREAM ingest ON TABLE sink
        | STMPROPERTIES(
        | 'trigger' = 'ProcessingTime',
        | 'interval' = '3 seconds')
        | AS SELECT * FROM source
      """.stripMargin)

    // start writing data into the socket
    import StructuredStreamingExample.{showTableCount, writeSocket}
    val thread1 = writeSocket(serverSocket, recordFormat)
    val thread2 = showTableCount(spark, "sink")

    System.out.println("type enter to interrupt streaming")
    System.in.read()
    thread1.interrupt()
    thread2.interrupt()
    serverSocket.close()

    // stop streaming job
    spark.sql("DROP STREAM ingest").show

    spark.stop()
    System.out.println("streaming finished")
  }

}

// scalastyle:on println
