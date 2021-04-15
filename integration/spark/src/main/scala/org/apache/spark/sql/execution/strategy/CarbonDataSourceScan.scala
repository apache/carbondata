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

import scala.collection.JavaConverters._

import org.apache.spark.CarbonInputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonToSparkAdapter}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{DataSourceScanExec, WholeStageCodegenExec}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.AtomicType

import org.apache.carbondata.core.index.IndexFilter
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.schema.BucketingInfo
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 *  Physical plan node for scanning data. It is applied for both tables
 *  USING carbondata and STORED AS carbondata.
 */
case class CarbonDataSourceScan(
    @transient relation: CarbonDatasourceHadoopRelation,
    output: Seq[Attribute],
    partitionFilters: Seq[SparkExpression],
    dataFilters: Seq[SparkExpression],
    @transient readComittedScope: ReadCommittedScope,
    @transient pushedDownProjection: CarbonProjection,
    @transient pushedDownFilters: Seq[Expression],
    directScanSupport: Boolean,
    @transient extraRDD: Option[(RDD[InternalRow], Boolean)] = None,
    tableIdentifier: Option[TableIdentifier] = None,
    segmentIds: Option[String] = None)
  extends DataSourceScanExec {

  lazy val supportsBatch: Boolean = {
    CarbonPlanHelper.supportBatchedDataSource(sqlContext, output, extraRDD)
  }

  lazy val supportsColumnar: Boolean = {
    CarbonPlanHelper.supportBatchedDataSource(sqlContext, output, extraRDD)
  }

  lazy val needsUnsafeRowConversion: Boolean = { true }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    val info: BucketingInfo = relation.carbonTable.getBucketingInfo
    if (info != null) {
      val cols = info.getListOfColumns.asScala
      val numBuckets = info.getNumOfRanges
      val bucketColumns = cols.flatMap { n =>
        val attrRef = output.find(_.name.equalsIgnoreCase(n.getColumnName))
        attrRef match {
          case Some(attr: AttributeReference) =>
            Some(AttributeReference(attr.name,
              CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(n.getDataType),
              attr.nullable,
              attr.metadata)(attr.exprId, attr.qualifier))
          case _ => None
        }
      }
      if (bucketColumns.size == cols.size) {
        // use HashPartitioning will not shuffle
        (HashPartitioning(bucketColumns, numBuckets), Nil)
      } else {
        (UnknownPartitioning(0), Nil)
      }
    } else {
      (UnknownPartitioning(0), Nil)
    }
  }

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val metadata =
      Map(
        "ReadSchema" -> seqToString(pushedDownProjection.getAllColumns),
        "Batched" -> supportsColumnar.toString,
        "DirectScan" -> (supportsColumnar && directScanSupport).toString,
        "PushedFilters" -> seqToString(pushedDownFilters.map(_.getStatement)))
    if (relation.carbonTable.isHivePartitionTable) {
      metadata + ("PartitionFilters" -> seqToString(partitionFilters)) +
        ("PartitionCount" -> selectedPartitions.size.toString)
    } else {
      metadata
    }
  }

  @transient private lazy val indexFilter: IndexFilter = {
    val filter = pushedDownFilters.reduceOption(new AndExpression(_, _))
      .map(new IndexFilter(relation.carbonTable, _, true)).orNull
    if (filter != null && pushedDownFilters.length == 1) {
      // push down the limit if only one filter
      filter.setLimit(relation.limit)
    }
    filter
  }

  @transient private lazy val selectedPartitions: Seq[PartitionSpec] = {
    CarbonFilters
      .getPartitions(partitionFilters, relation.sparkSession, relation.carbonTable)
      .orNull
  }

  private lazy val inputRDD: RDD[InternalRow] = {
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
    carbonRdd.setVectorReaderSupport(supportsColumnar)
    carbonRdd.setDirectScanSupport(supportsColumnar && directScanSupport)
    extraRDD.map(_._1.union(carbonRdd)).getOrElse(carbonRdd)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = inputRDD :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    if (supportsColumnar) {
      // in the case of fallback, this batched scan should never fail because of:
      // 1) only primitive types are supported
      // 2) the number of columns should be smaller than spark.sql.codegen.maxFields
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val unsafeRows = {
        val scan = inputRDD
        if (needsUnsafeRowConversion) {
          scan.mapPartitionsWithIndexInternal { (index, iter) =>
            val proj = UnsafeProjection.create(schema)
            proj.initialize(index)
            iter.map(proj)
          }
        } else {
          scan
        }
      }
      val numOutputRows = longMetric("numOutputRows")
      unsafeRows.map { r =>
        numOutputRows += 1
        r
      }
    }
  }

  override protected def doCanonicalize(): CarbonDataSourceScan = {
    CarbonDataSourceScan(
      relation,
      output.map(CarbonToSparkAdapter.normalizeExpressions(_, output)),
      QueryPlan.normalizePredicates(partitionFilters, output),
      QueryPlan.normalizePredicates(dataFilters, output),
      null,
      null,
      null,
      directScanSupport,
      extraRDD,
      tableIdentifier)
  }

  protected def doProduce(ctx: CodegenContext): String = {
    WholeStageCodegenExec(this)(1).doProduce(ctx)
  }
}
