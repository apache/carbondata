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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier

/**
 * For handling operation's after finish of index creation over table with index datamap
 * example: bloom datamap, Lucene datamap
 */
case class CreateDataMapPostExecutionEvent(sparkSession: SparkSession,
    storePath: String, tableIdentifier: Option[TableIdentifier], dmProviderName: String)
  extends Event with CreateDataMapEventsInfo

/**
 * For handling operation's before start of update index datmap status over table with index datamap
 * example: bloom datamap, Lucene datamap
 */
case class UpdateDataMapPreExecutionEvent(sparkSession: SparkSession,
    storePath: String, tableIdentifier: TableIdentifier)
  extends Event with CreateDataMapEventsInfo

/**
 * For handling operation's after finish of  update index datmap status over table with index
 * datamap
 * example: bloom datamap, Lucene datamap
 */
case class UpdateDataMapPostExecutionEvent(sparkSession: SparkSession,
    storePath: String, tableIdentifier: TableIdentifier)
  extends Event with CreateDataMapEventsInfo

/**
 * For handling operation's before start of index build over table with index datamap
 * example: bloom datamap, Lucene datamap
 */
case class BuildDataMapPreExecutionEvent(sparkSession: SparkSession,
    identifier: AbsoluteTableIdentifier, dataMapNames: scala.collection.mutable.Seq[String])
  extends Event with BuildDataMapEventsInfo

/**
 * For handling operation's after finish of index build over table with index datamap
 * example: bloom datamap, Lucene datamap
 *
 * @param sparkSession
 * @param identifier
 * @param dmName set to specify datamap name in rebuild process;
 *               set to Null in loading and compaction and it will deal all datamaps
 * @param segmentIdList
 * @param isFromRebuild set to false in loading process for skipping lazy datamap
 */
case class BuildDataMapPostExecutionEvent(sparkSession: SparkSession,
    identifier: AbsoluteTableIdentifier, dmName: String,
    segmentIdList: Seq[String], isFromRebuild: Boolean)
  extends Event with TableEventInfo

/**
 * For handling operation's before start of index creation over table with index datamap
 * example: bloom datamap, Lucene datamap
 */
case class CreateDataMapPreExecutionEvent(sparkSession: SparkSession,
    storePath: String, tableIdentifier: TableIdentifier)
  extends Event with CreateDataMapEventsInfo

