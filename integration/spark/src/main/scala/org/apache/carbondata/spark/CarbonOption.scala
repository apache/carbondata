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

package org.apache.carbondata.spark


/**
 * Contains all options for Spark data source
 */
class CarbonOption(options: Map[String, String]) {

  lazy val dbName: Option[String] = options.get("dbName")

  lazy val tableName: String = options.getOrElse("tableName", "default_table")

  lazy val tablePath: Option[String] = options.get("tablePath")

  lazy val partitionCount: String = options.getOrElse("partitionCount", "1")

  lazy val partitionClass: String = {
    options.getOrElse("partitionClass",
      "org.apache.carbondata.processing.partition.impl.SampleDataPartitionerImpl")
  }

  lazy val compress: Boolean = options.getOrElse("compress", "false").toBoolean

  lazy val partitionColumns: Option[Seq[String]] = {
    if (options.contains("partitionColumns")) {
      Option(options("partitionColumns").split(",").map(_.trim))
    } else {
      None
    }
  }

  lazy val sortColumns: Option[String] = options.get("sort_columns")

  lazy val sortScope: Option[String] = options.get("sort_scope")

  lazy val longStringColumns: Option[String] = options.get("long_string_columns")

  lazy val tableBlockSize: Option[String] = options.get("table_blocksize")

  lazy val tableBlockletSize: Option[String] = options.get("table_blocklet_size")

  lazy val tablePageSizeInMb: Option[String] = options.get("table_page_size_inmb")

  lazy val bucketNumber: Int = options.getOrElse("bucketnumber", "0").toInt

  lazy val bucketColumns: String = options.getOrElse("bucketcolumns", "")

  lazy val isBucketingEnabled: Boolean = options.contains("bucketcolumns") &&
                                    options.contains("bucketnumber")

  lazy val isStreaming: Boolean = {
    var stream = options.getOrElse("streaming", "false")
    if (stream.equalsIgnoreCase("sink") || stream.equalsIgnoreCase("source")) {
      stream = "true"
    }
    stream.toBoolean
  }

  lazy val overwriteEnabled: Boolean =
    options.getOrElse("overwrite", "false").toBoolean

  def toMap: Map[String, String] = options
}
