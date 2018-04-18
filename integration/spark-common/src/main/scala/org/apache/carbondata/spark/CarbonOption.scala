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

  def dbName: Option[String] = options.get("dbName")

  def tableName: String = options.getOrElse("tableName", "default_table")

  def tablePath: Option[String] = options.get("tablePath")

  def partitionCount: String = options.getOrElse("partitionCount", "1")

  def partitionClass: String = {
    options.getOrElse("partitionClass",
      "org.apache.carbondata.processing.partition.impl.SampleDataPartitionerImpl")
  }

  def tempCSV: Boolean = options.getOrElse("tempCSV", "false").toBoolean

  def compress: Boolean = options.getOrElse("compress", "false").toBoolean

  def singlePass: Boolean = options.getOrElse("single_pass", "false").toBoolean

  def sortColumns: Option[String] = options.get("sort_columns")

  def dictionaryInclude: Option[String] = options.get("dictionary_include")

  def dictionaryExclude: Option[String] = options.get("dictionary_exclude")

  def tableBlockSize: Option[String] = options.get("table_blocksize")

  def bucketNumber: Int = options.getOrElse("bucketnumber", "0").toInt

  def bucketColumns: String = options.getOrElse("bucketcolumns", "")

  def isBucketingEnabled: Boolean = options.contains("bucketcolumns") &&
                                    options.contains("bucketnumber")

  def isStreaming: Boolean =
    options.getOrElse("streaming", "false").toBoolean

  def toMap: Map[String, String] = options
}
