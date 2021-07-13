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
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression => SparkExpression, PlanExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.{DataSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.carbondata.core.index.IndexFilter
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonScanRDD

abstract class CarbonDataSourceScanHelper(relation: CarbonDatasourceHadoopRelation,
    output: Seq[Attribute],
    partitionFiltersWithoutDpp: Seq[SparkExpression],
    pushedDownFilters: Seq[Expression],
    pushedDownProjection: CarbonProjection,
    directScanSupport: Boolean,
    extraRDD: Option[(RDD[InternalRow], Boolean)],
    selectedCatalogPartitions: Seq[CatalogTablePartition],
    partitionFiltersWithDpp: Seq[SparkExpression],
    segmentIds: Option[String])
  extends DataSourceScanExec {

  override lazy val supportsColumnar: Boolean = CarbonPlanHelper
    .supportBatchedDataSource(sqlContext, output, extraRDD)

  lazy val supportsBatchOrColumnar: Boolean = supportsColumnar

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  lazy val needsUnsafeRowConversion: Boolean = true

  val outputAttibutesAfterNormalizingExpressionIds: Seq[Attribute] = output
      .map(CarbonToSparkAdapter.normalizeExpressions(_, output))

  @transient lazy val indexFilter: IndexFilter = {
    val filter = pushedDownFilters.reduceOption(new AndExpression(_, _))
      .map(new IndexFilter(relation.carbonTable, _, true)).orNull
    if (filter != null && pushedDownFilters.length == 1) {
      // push down the limit if only one filter
      filter.setLimit(relation.getLimit)
    }
    filter
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          val res = batches.hasNext
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  @transient lazy val selectedPartitions: Seq[PartitionSpec] = {
    CarbonFilters
      .getPartitions(partitionFiltersWithoutDpp, relation.sparkSession, relation.carbonTable)
      .orNull
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val dynamicFilter = partitionFiltersWithDpp.filter(exp =>
      exp.find(_.isInstanceOf[PlanExpression[_]]).isDefined)
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
    if(dynamicFilter.nonEmpty) {
      carbonRdd match {
        case carbonRdd: CarbonScanRDD[InternalRow] =>
          // prune dynamic partitions based on filter
          val sparkExpression = SparkSQLUtil.getSparkSession
          val runtimePartitions = ExternalCatalogUtils.prunePartitionsByFilter(
            sparkExpression.sessionState.catalog.getTableMetadata(tableIdentifier.get),
            selectedCatalogPartitions,
            dynamicFilter,
            sparkExpression.sessionState.conf.sessionLocalTimeZone
          )
          // set partitions to carbon rdd
          carbonRdd.partitionNames = CarbonFilters.convertToPartitionSpec(runtimePartitions)
      }
    }
    carbonRdd.setVectorReaderSupport(supportsColumnar)
    carbonRdd.setDirectScanSupport(supportsColumnar && directScanSupport)
    extraRDD.map(_._1.union(carbonRdd)).getOrElse(carbonRdd)
  }

  def doProduce(ctx: CodegenContext): String = {
    WholeStageCodegenExec(this)(1).doProduce(ctx)
  }
}
