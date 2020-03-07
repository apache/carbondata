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

package org.apache.carbondata.view

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.events.Event

/**
 * For handling operation's before start of mv creation
 */
case class CreateMaterializedViewPreExecutionEvent(sparkSession: SparkSession,
    tableIdentifier: TableIdentifier) extends Event

/**
 * For handling operation's after finish of mv creation
 */
case class CreateMaterializedViewPostExecutionEvent(sparkSession: SparkSession,
    tableIdentifier: TableIdentifier) extends Event

/**
 * For handling operation's before start of update mv status
 */
case class UpdateMaterializedViewPreExecutionEvent(sparkSession: SparkSession,
    tableIdentifier: TableIdentifier) extends Event

/**
 * For handling operation's after finish of update mv status
 */
case class UpdateMaterializedViewPostExecutionEvent(sparkSession: SparkSession,
    tableIdentifier: TableIdentifier) extends Event

/**
 * For handling operation's before start of mv refresh
 */
case class RefreshMaterializedViewPreExecutionEvent(sparkSession: SparkSession,
    tableIdentifier: TableIdentifier) extends Event

/**
 * For handling operation's after finish of mv refresh
 */
// TODO refreshed segment ids, auto or manual
case class RefreshMaterializedViewPostExecutionEvent(sparkSession: SparkSession,
    tableIdentifier: TableIdentifier) extends Event
