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
package org.apache.carbondata.spark.rdd

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.datamap.DataMapFilter
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.DataTypeConverter
import org.apache.carbondata.hadoop.{CarbonMultiBlockSplit, CarbonProjection}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport
import org.apache.carbondata.spark.InitInputMetrics

/**
 * It can get the deleted/updated records on any particular update version. It is useful to get the
 * records changed on any particular update transaction.
 */
class CarbonDeltaRowScanRDD[T: ClassTag](
    @transient private val spark: SparkSession,
    @transient private val serializedTableInfo: Array[Byte],
    @transient private val tableInfo: TableInfo,
    @transient override val partitionNames: Seq[PartitionSpec],
    override val columnProjection: CarbonProjection,
    var filter: DataMapFilter,
    identifier: AbsoluteTableIdentifier,
    inputMetricsStats: InitInputMetrics,
    override val dataTypeConverterClz: Class[_ <: DataTypeConverter] =
    classOf[SparkDataTypeConverterImpl],
    override val readSupportClz: Class[_ <: CarbonReadSupport[_]] =
    SparkReadSupport.readSupportClass,
    deltaVersionToRead: String) extends
  CarbonScanRDD[T](
    spark,
    columnProjection,
    filter,
    identifier,
    serializedTableInfo,
    tableInfo,
    inputMetricsStats,
    partitionNames,
    dataTypeConverterClz,
    readSupportClz) {
  override def internalGetPartitions: Array[Partition] = {
    val table = CarbonTable.buildFromTableInfo(getTableInfo)
    val updateStatusManager = new SegmentUpdateStatusManager(table, deltaVersionToRead)

    val parts = super.internalGetPartitions
    parts.map { p =>
      val partition = p.asInstanceOf[CarbonSparkPartition]
      val splits = partition.multiBlockSplit.getAllSplits.asScala.filter { s =>
        updateStatusManager.getDetailsForABlock(
          CarbonUpdateUtil.getSegmentBlockNameKey(s.getSegmentId, s.getBlockPath)) != null
      }.asJava
      new CarbonSparkPartition(partition.rddId, partition.index,
        new CarbonMultiBlockSplit(splits, partition.multiBlockSplit.getLocations))
    }.filter(p => p.multiBlockSplit.getAllSplits.size() > 0).asInstanceOf[Array[Partition]]
  }

  override def createInputFormat(conf: Configuration): CarbonTableInputFormat[Object] = {
    val format = super.createInputFormat(conf)
    conf.set("updateDeltaVersion", deltaVersionToRead)
    conf.set("readDeltaOnly", "true")
    format
  }
}
