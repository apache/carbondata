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

import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}


/**
 * This event is fired before creating datamap
 * @param ifExistsSet
 * @param sparkSession
 */
case class DropDataMapPreEvent(
    dataMapSchema: Option[DataMapSchema],
    ifExistsSet: Boolean,
    sparkSession: SparkSession) extends Event


/**
 * This event is fired after creating datamap.
 * @param ifExistsSet
 * @param sparkSession
 */
case class DropDataMapPostEvent(
    dataMapSchema: Option[DataMapSchema],
    ifExistsSet: Boolean,
    sparkSession: SparkSession) extends Event


/**
 * This event is fired when any abort operation during datamap creation.
 * @param ifExistsSet
 * @param sparkSession
 */
case class DropDataMapAbortEvent(
    dataMapSchema: Option[DataMapSchema],
    ifExistsSet: Boolean,
    sparkSession: SparkSession) extends Event
