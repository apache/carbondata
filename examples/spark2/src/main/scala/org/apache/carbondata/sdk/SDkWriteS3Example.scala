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
package org.apache.carbondata.sdk

import org.slf4j.{Logger, LoggerFactory}

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.sdk.file.{CarbonReader, CarbonWriter, Field, Schema}

/**
 * Generate data and write data to S3 by SDK, no spark
 */
object SDKWriteS3Example {

  // scalastyle:off println
  /**
   * This example demonstrate usage of
   *
   * @param args require three parameters "Access-key" "Secret-key"
   *             "s3-endpoint", other is optional
   */
  def main(args: Array[String]) {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    if (args.length < 3 || args.length > 6) {
      logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>" +
        "<s3-endpoint> [table-path-on-s3] [number-of-rows] [persistSchema]")
      System.exit(0)
    }

    val path = if (args.length > 3) {
      args(3)
    } else {
      "s3a://sdk/WriterOutput"
    }

    val num = if (args.length > 4) {
      Integer.parseInt(args(4))
    } else {
      3
    }

    val persistSchema = if (args.length > 5) {
      if (args(5).equalsIgnoreCase("true")) {
        true
      } else {
        false
      }
    } else {
      true
    }

    // getCanonicalPath gives path with \, so code expects /.
    val writerPath = path.replace("\\", "/");

    val fields: Array[Field] = new Array[Field](3)
    fields(0) = new Field("name", DataTypes.STRING)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)

    try {
      val builder = CarbonWriter.builder()
        .withSchema(new Schema(fields))
        .outputPath(writerPath)
        .isTransactionalTable(true)
        .uniqueIdentifier(System.currentTimeMillis)
        .setAccessKey(args(0))
        .setSecretKey(args(1))
        .setEndPoint(args(2))
      val writer =
        if (persistSchema) {
          builder.persistSchemaFile(true)
            .buildWriterForCSVInput()
        } else {
          builder
            .withBlockSize(2)
            .buildWriterForCSVInput()
        }
      var i = 0
      var row = num
      while (i < row) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }

    println("Write Finished")

    // getCanonicalPath gives path with \, so code expects /.
    val readPath = path.replace("\\", "/");
    val reader = CarbonReader
      .builder(readPath, "_temp")
      .projection(Array[String]("name", "age", "height"))
      .build

    var i = 0
    while (i < 10 && reader.hasNext) {
      val row = reader.readNextRow.asInstanceOf[Array[AnyRef]]
      println(row(0) + " " + row(1) + " " + row(2))
      i = i + 1
    }

    // TODO: write PR2221 merge
    //    reader.close
  }

  // scalastyle:on println
}
