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
package org.apache.spark.sql.execution.strategy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 *  Physical plan node for scanning data. It is applied for both tables
 *  USING carbondata and STORED AS CARBONDATA.
 */
class CarbonDataSourceScan(
    override val output: Seq[Attribute],
    val rdd: RDD[InternalRow],
    @transient override val relation: HadoopFsRelation,
    val partitioning: Partitioning,
    override val metadata: Map[String, String],
    identifier: Option[TableIdentifier],
    @transient private val logicalRelation: LogicalRelation)
  extends FileSourceScanExec(
    relation,
    output,
    relation.dataSchema,
    Seq.empty,
    Seq.empty,
    identifier) {

  override val supportsBatch: Boolean = true

  override val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) =
    (partitioning, Nil)

  override def inputRDDs(): Seq[RDD[InternalRow]] = rdd :: Nil

}
