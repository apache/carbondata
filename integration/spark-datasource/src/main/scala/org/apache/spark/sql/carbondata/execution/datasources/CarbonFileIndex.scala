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
package org.apache.spark.sql.carbondata.execution.datasources

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, _}
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.core.scan.expression.{Expression => CarbonExpression}
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, CarbonInputFormat}

/**
 * Its a custom implementation which uses carbon's driver pruning feature to prune carbondata files
 * using carbonindex.
 */
class CarbonFileIndex(
    sparkSession: SparkSession,
    dataSchema: StructType,
    parameters: Map[String, String],
    inMemoryFileIndex: InMemoryFileIndex)
  extends FileIndex with AbstractCarbonFileIndex {

  override def rootPaths: Seq[Path] = inMemoryFileIndex.rootPaths

  override def inputFiles: Array[String] = inMemoryFileIndex.inputFiles

  override def refresh(): Unit = inMemoryFileIndex.refresh()

  override def sizeInBytes: Long = inMemoryFileIndex.sizeInBytes

  override def partitionSchema: StructType = inMemoryFileIndex.partitionSchema

  /**
   * It lists the pruned files after applying partition and data filters.
   *
   * @param partitionFilters
   * @param dataFilters
   * @return
   */
  override def listFiles(partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val method = inMemoryFileIndex.getClass.getMethods.find(_.getName == "listFiles").get
    val directories =
      method.invoke(
        inMemoryFileIndex,
        partitionFilters,
        dataFilters).asInstanceOf[Seq[PartitionDirectory]]
    prune(dataFilters, directories)
  }

  private def prune(dataFilters: Seq[Expression],
      directories: Seq[PartitionDirectory]) = {
    val tablePath = parameters.get("path")
    if (tablePath.nonEmpty) {
      val hadoopConf = new Configuration(sparkSession.sparkContext.hadoopConfiguration)
      // convert t sparks source filter
      val filters = dataFilters.flatMap(DataSourceStrategy.translateFilter)

      // convert to carbon filter expressions
      val filter: Option[CarbonExpression] = filters.flatMap { filter =>
        CarbonSparkDataSourceUtil.createCarbonFilter(dataSchema, filter)
      }.reduceOption(new AndExpression(_, _))
      val model = CarbonSparkDataSourceUtil.prepareLoadModel(parameters, dataSchema)
      CarbonInputFormat.setTableInfo(
        hadoopConf,
        model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
      CarbonInputFormat.setTransactionalTable(hadoopConf, false)
      if (rootPaths.nonEmpty) {
        // Check for any subfolders are present here.
        if (!rootPaths.head.equals(new Path(tablePath.get)) &&
            rootPaths.head.toString.contains(tablePath.get)) {
          CarbonInputFormat.setDataFoldersToRead(hadoopConf,
            rootPaths.map(_.toUri.toString).toArray)
        }
      }
      filter match {
        case Some(c) => CarbonInputFormat.setFilterPredicates(hadoopConf, c)
        case None => None
      }
      val format: CarbonFileInputFormat[Object] = new CarbonFileInputFormat[Object]
      val jobConf = new JobConf(hadoopConf)
      SparkHadoopUtil.get.addCredentials(jobConf)
      val splits = format.getSplits(Job.getInstance(jobConf))
        .asInstanceOf[util.List[CarbonInputSplit]].asScala
      val prunedDirs = directories.map { dir =>
        val files = dir.files
          .filter(d => splits.exists(_.getBlockPath.equalsIgnoreCase(d.getPath.getName)))
        PartitionDirectory(dir.values, files)
      }
      prunedDirs
    } else {
      directories
    }
  }

  override def listFiles(filters: Seq[Expression]): Seq[PartitionDirectory] = {
    val method = inMemoryFileIndex.getClass.getMethods.find(_.getName == "listFiles").get
    val directories =
      method.invoke(inMemoryFileIndex, filters).asInstanceOf[Seq[PartitionDirectory]]
    prune(filters, directories)
  }
}

/**
 * It is a just class to make compile between spark 2.1 and 2.2
 */
trait AbstractCarbonFileIndex {

  def listFiles(partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory]

  def listFiles(filters: Seq[Expression]): Seq[PartitionDirectory]
}
