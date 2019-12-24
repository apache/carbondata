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

package org.apache.carbondata.events

import org.apache.spark.sql._

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo

/**
 * Class for handling operations before start of a load process.
 * Example usage: For validation purpose
 */
case class CreateTablePreExecutionEvent(
    sparkSession: SparkSession,
    identifier: AbsoluteTableIdentifier,
    tableInfo: Option[TableInfo]) extends Event with TableEventInfo

/**
 * Class for handling operations after data load completion and before final
 * commit of load operation.
 */
case class CreateTablePostExecutionEvent(sparkSession: SparkSession,
    identifier: AbsoluteTableIdentifier) extends Event with TableEventInfo

/**
 * Class for handling clean up in case of any failure and abort the operation.
 */
case class CreateTableAbortExecutionEvent(sparkSession: SparkSession,
    identifier: AbsoluteTableIdentifier) extends Event with TableEventInfo
