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
 * Read data from S3 by SDK, no spark
 */
object SDKReadS3Example {

  // scalastyle:off println
  /**
   * This example demonstrate usage of
   *
   * @param args require three parameters "Access-key" "Secret-key"
   *             "s3-endpoint", other is optional
   */
  def main(args: Array[String]) {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    if (args.length < 3 || args.length > 4) {
      logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>" +
        "<s3-endpoint> [table-path-on-s3] ")
      System.exit(0)
    }

    val path = if (args.length > 3) {
      args(3)
    } else {
      "s3a://sdk/WriterOutput"
    }

    // getCanonicalPath gives path with \, so code expects /.
    val readPath = path.replace("\\", "/");

    val reader = CarbonReader
      .builder(readPath, "_temp")
      .projection(Array[String]("name", "age", "height"))
      .setAccessKey(args(0))
      .setSecretKey(args(1))
      .setEndPoint(args(2)).build

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
