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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}

/**
 * Running SQL on carbon files directly
 * No need to create table first
 * TODO: support more than one carbon file
 */
// scalastyle:off println
object DirectSQLExample {

  def main(args: Array[String]) {
    val carbonSession = ExampleUtils.createSparkSession("DirectSQLExample")
    exampleBody(carbonSession)
    carbonSession.close()
  }

  def exampleBody(carbonSession : SparkSession): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val path = s"$rootPath/examples/spark2/target/carbonFile/"

    import carbonSession._
    // 1. generate data file
    cleanTestData(path)

    val rows = 20
    buildTestData(path, rows)
    val readPath = path

    println("Running SQL on carbon files directly")
    try {
      // 2. run queries directly, no need to create table first
      sql(s"""select * FROM carbon.`$readPath` limit 10""".stripMargin).show()

      // 3. check rows count
      val counts = sql(s"""select * FROM carbon.`$readPath`""".stripMargin).count()
      assert(rows == counts)

    } catch {
      case e: Exception => throw e
    } finally {
      // 3.delete data files
      cleanTestData(path)
    }

  }

  // prepare SDK writer output
  def buildTestData(
      path: String,
      num: Int = 3): Unit = {

    // getCanonicalPath gives path with \, but the code expects /.
    val writerPath = path.replace("\\", "/")

    val fields = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)

    try {
      val builder = CarbonWriter
        .builder()
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis)
        .withBlockSize(2)
        .withCsvInput(new Schema(fields))
        .writtenBy("DirectSQLExample")
      val writer = builder.build()
      var i = 0
      while (i < num) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case e: Exception => throw e
    }
  }

  def cleanTestData(path: String): Unit = {
    FileUtils.deleteDirectory(new File(path))
  }

}
// scalastyle:on println
