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
  def tableIdentifier: String = options.getOrElse("tableName", s"$dbName.$tableName")

  def dbName: String = options.getOrElse("dbName", "default")

  def tableName: String = options.getOrElse("tableName", "default_table")

  def tableId: String = options.getOrElse("tableId", "default_table_id")

  def partitionCount: String = options.getOrElse("partitionCount", "1")

  def partitionClass: String = {
    options.getOrElse("partitionClass",
      "org.apache.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl")
  }

  def compress: String = options.getOrElse("compress", "false")

}
