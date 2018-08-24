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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InMemoryFileIndex, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Rule to replace FileIndex with CarbonFileIndex for better driver pruning.
 */
class CarbonFileIndexReplaceRule extends Rule[LogicalPlan] {

  /**
   * This property creates subfolder for every load
   */
  private val createSubFolder = CarbonProperties.getInstance()
    .getProperty("carbonfileformat.create.folder.perload", "false").toBoolean

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val transformedPlan = plan.transform {
      case l: LogicalRelation
        if l.relation.isInstanceOf[HadoopFsRelation] &&
           l.relation.asInstanceOf[HadoopFsRelation].fileFormat.toString.equals("carbon") &&
           !l.relation.asInstanceOf[HadoopFsRelation].location.isInstanceOf[CarbonFileIndex] =>
        val fsRelation = l.relation.asInstanceOf[HadoopFsRelation]
        val fileIndex = fsRelation.location
        val carbonFileIndex = new CarbonFileIndex(fsRelation.sparkSession,
          fsRelation.schema,
          fsRelation.options,
          updateFileIndex(fileIndex, fsRelation))
        val fsRelationCopy = fsRelation.copy(location = carbonFileIndex)(fsRelation.sparkSession)
        val logicalRelation = l.copy(relation = fsRelationCopy.asInstanceOf[BaseRelation])
        logicalRelation
      case insert: InsertIntoHadoopFsRelationCommand
        if createSubFolder && insert.fileFormat.toString.equals("carbon") &&
           FileFactory.getUpdatedFilePath(insert.outputPath.toString).equals(
             FileFactory.getUpdatedFilePath(insert.options("path"))) &&
           insert.partitionColumns.isEmpty =>
        val path = new Path(insert.outputPath, System.nanoTime().toString)
        insert.copy(outputPath = path)
    }
    transformedPlan
  }

  private def updateFileIndex(fileIndex: FileIndex,
      hadoopFsRelation: HadoopFsRelation): FileIndex = {
    if (fileIndex.isInstanceOf[InMemoryFileIndex] && fileIndex.rootPaths.length == 1) {
      val carbonFile = FileFactory.getCarbonFile(fileIndex.rootPaths.head.toUri.toString)
      val carbonFiles = carbonFile.listFiles()
      if (carbonFiles.nonEmpty &&
          !carbonFiles.exists(_.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT))) {
        val paths = carbonFiles.map(p => new Path(p.getAbsolutePath)).toSeq
        new InMemoryFileIndex(hadoopFsRelation.sparkSession,
          paths,
          hadoopFsRelation.options,
          Some(hadoopFsRelation.partitionSchema))
      } else {
        fileIndex
      }
    } else {
      fileIndex
    }
  }
}
