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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{AtomicType, StructType}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapFilter
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, HDFSCarbonFile}
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope
import org.apache.carbondata.core.scan.expression.{Expression => CarbonExpression}
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
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
    fileIndex: FileIndex)
  extends FileIndex with AbstractCarbonFileIndex {

  // When this flag is set it just returns empty files during pruning. It is needed for carbon
  // session partition flow as we handle directly through datamap pruining.
  private var actAsDummy = false

  override def rootPaths: Seq[Path] = fileIndex.rootPaths

  override def inputFiles: Array[String] = fileIndex.inputFiles

  override def refresh(): Unit = fileIndex.refresh()

  override def sizeInBytes: Long = fileIndex.sizeInBytes

  override def partitionSchema: StructType = fileIndex.partitionSchema

  /**
   * It lists the pruned files after applying partition and data filters.
   *
   * @param partitionFilters
   * @param dataFilters
   * @return
   */
  override def listFiles(partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (actAsDummy) {
      return Seq.empty
    }
    val method = fileIndex.getClass.getMethods.find(_.getName == "listFiles").get
    val directories =
      method.invoke(
        fileIndex,
        partitionFilters,
        dataFilters).asInstanceOf[Seq[PartitionDirectory]]
    prune(dataFilters, directories)
  }

  private def prune(dataFilters: Seq[Expression],
      directories: Seq[PartitionDirectory]): Seq[PartitionDirectory] = {
    // set the driver flag to true which will used for unsafe memory initialization and carbon LRU
    // cache instance initialization as per teh driver memory
    CarbonProperties.getInstance
      .addNonSerializableProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "true")
    val tablePath = parameters.get("path")
    if (tablePath.nonEmpty && dataFilters.nonEmpty) {
      val hadoopConf = sparkSession.sessionState.newHadoopConf()
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(hadoopConf)
      // convert t sparks source filter
      val filters = dataFilters.flatMap(DataSourceStrategy.translateFilter)
      val dataTypeMap = dataSchema.map(f => f.name -> f.dataType).toMap
      // convert to carbon filter expressions
      val filter: Option[CarbonExpression] = filters.filterNot{ ref =>
        ref.references.exists{ p =>
          !dataTypeMap(p).isInstanceOf[AtomicType]
        }
      }.flatMap { filter =>
        CarbonSparkDataSourceUtil.createCarbonFilter(dataSchema, filter)
      }.reduceOption(new AndExpression(_, _))
      val model = CarbonSparkDataSourceUtil.prepareLoadModel(parameters, dataSchema)
      CarbonInputFormat.setTableInfo(
        hadoopConf,
        model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
      CarbonInputFormat.setTransactionalTable(hadoopConf, false)
      var totalFiles = 0
      val indexFiles = directories.flatMap { dir =>
        totalFiles += dir.files.length
        dir.files.filter{f =>
          f.getPath.getName.endsWith(CarbonTablePath.INDEX_FILE_EXT) ||
          f.getPath.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)}.
          map(new HDFSCarbonFile(_))
      }.toArray.asInstanceOf[Array[CarbonFile]]
      if (indexFiles.length == 0 && totalFiles > 0) {
        return directories
      }
      CarbonInputFormat.setReadCommittedScope(
        hadoopConf,
        new LatestFilesReadCommittedScope(indexFiles, hadoopConf))
      filter match {
        case Some(c) => CarbonInputFormat
          .setFilterPredicates(hadoopConf,
            new DataMapFilter(model.getCarbonDataLoadSchema.getCarbonTable, c, true))
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
      directories.map { dir =>
        val files = dir.files
          .filter(_.getPath.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT))
        PartitionDirectory(dir.values, files)
      }
    }
  }

  override def listFiles(filters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (actAsDummy) {
      return Seq.empty
    }
    val method = fileIndex.getClass.getMethods.find(_.getName == "listFiles").get
    val directories =
      method.invoke(fileIndex, filters).asInstanceOf[Seq[PartitionDirectory]]
    prune(filters, directories)
  }

  def setDummy(actDummy: Boolean): Unit = {
    actAsDummy = actDummy
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
