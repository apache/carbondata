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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil

case class CarbonCountStar(
    attributesRaw: Seq[Attribute],
    carbonTable: CarbonTable,
    sparkSession: SparkSession,
    outUnsafeRows: Boolean = true) extends LeafExecNode {

  override def doExecute(): RDD[InternalRow] = {
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val (job, tableInputFormat) = createCarbonInputFormat(absoluteTableIdentifier)
    CarbonInputFormat.setQuerySegment(job.getConfiguration, carbonTable)

    // get row count
    val rowCount = CarbonUpdateUtil.getRowCount(
      tableInputFormat.getBlockRowCount(
        job,
        carbonTable,
        CarbonFilters.getPartitions(
          Seq.empty,
          sparkSession,
          TableIdentifier(
            carbonTable.getTableName,
            Some(carbonTable.getDatabaseName))).map(_.asJava).orNull, false),
      carbonTable)
    val valueRaw =
      attributesRaw.head.dataType match {
        case StringType => Seq(UTF8String.fromString(Long.box(rowCount).toString)).toArray
          .asInstanceOf[Array[Any]]
        case _ => Seq(Long.box(rowCount)).toArray.asInstanceOf[Array[Any]]
      }
    val value = new GenericInternalRow(valueRaw)
    val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val row = if (outUnsafeRows) unsafeProjection(value) else value
    sparkContext.parallelize(Seq(row))
  }

  override def output: Seq[Attribute] = {
    attributesRaw
  }

  private def createCarbonInputFormat(absoluteTableIdentifier: AbsoluteTableIdentifier
  ): (Job, CarbonTableInputFormat[Array[Object]]) = {
    val carbonInputFormat = new CarbonTableInputFormat[Array[Object]]()
    val jobConf: JobConf = new JobConf(FileFactory.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    CarbonInputFormat.setTableInfo(jobConf, carbonTable.getTableInfo)
    val job = new Job(jobConf)
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    CarbonInputFormat
      .setTransactionalTable(job.getConfiguration,
        carbonTable.getTableInfo.isTransactionalTable)
    CarbonInputFormatUtil.setDataMapJobIfConfigured(job.getConfiguration)
    (job, carbonInputFormat)
  }
}
