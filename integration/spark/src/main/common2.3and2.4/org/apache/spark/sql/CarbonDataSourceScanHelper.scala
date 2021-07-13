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

package org.apache.spark.sql

import org.apache.spark.CarbonInputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression => SparkExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{ColumnarBatchScan, DataSourceScanExec}
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.core.index.IndexFilter
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonScanRDD

abstract class  CarbonDataSourceScanHelper(relation: CarbonDatasourceHadoopRelation,
    output: Seq[Attribute],
    partitionFilters: Seq[SparkExpression],
    pushedDownFilters: Seq[Expression],
    pushedDownProjection: CarbonProjection,
    directScanSupport: Boolean,
    extraRDD: Option[(RDD[InternalRow], Boolean)],
    selectedCatalogPartitions: Seq[CatalogTablePartition],
    partitionFilterWithDpp: Seq[SparkExpression],
    segmentIds: Option[String])
  extends DataSourceScanExec with ColumnarBatchScan {

  override lazy val supportsBatch: Boolean = {
    CarbonPlanHelper.supportBatchedDataSource(sqlContext, output, extraRDD)
  }

  lazy val supportsBatchOrColumnar: Boolean = supportsBatch

  val outputAttibutesAfterNormalizingExpressionIds: Seq[Attribute] = output
      .map(QueryPlan.normalizeExprId(_, output))

  @transient lazy val indexFilter: IndexFilter = {
    val filter = pushedDownFilters.reduceOption(new AndExpression(_, _))
      .map(new IndexFilter(relation.carbonTable, _, true)).orNull
    if (filter != null && pushedDownFilters.length == 1) {
      // push down the limit if only one filter
      filter.setLimit(relation.limit)
    }
    filter
  }

  @transient lazy val selectedPartitions: Seq[PartitionSpec] = {
    CarbonFilters
      .getPartitions(partitionFilters, relation.sparkSession, relation.carbonTable)
      .orNull
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val carbonRdd = new CarbonScanRDD[InternalRow](
      relation.sparkSession,
      pushedDownProjection,
      indexFilter,
      relation.identifier,
      relation.carbonTable.getTableInfo.serialize(),
      relation.carbonTable.getTableInfo,
      new CarbonInputMetrics,
      selectedPartitions,
      segmentIds = segmentIds)
    carbonRdd.setVectorReaderSupport(supportsBatch)
    carbonRdd.setDirectScanSupport(supportsBatch && directScanSupport)
    extraRDD.map(_._1.union(carbonRdd)).getOrElse(carbonRdd)
  }
}
