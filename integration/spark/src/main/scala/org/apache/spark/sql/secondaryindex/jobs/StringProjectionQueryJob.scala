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

package org.apache.spark.sql.secondaryindex.jobs

import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.core.index.AbstractIndexJob

/**
 * Spark job to run the sql and get the values of string projection column.
 * Note: Expects a string column as projection in sql.
 */
class StringProjectionQueryJob extends AbstractIndexJob {
  override def execute(sql: String): Array[Object] = {
    SparkSQLUtil
      .getSparkSession
      .sql(sql)
      .rdd
      .map(row => row.get(0).asInstanceOf[String])
      .collect
      .asInstanceOf[Array[Object]]
  }
}
