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
package org.apache.spark.sql.execution.command.mutation.merge

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.mutate.FilePathMinMaxVO
import org.apache.carbondata.core.util.{ByteUtil, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.comparator.SerializableComparator

/**
 * The utility class for Merge operations
 */
object CarbonMergeDataSetUtil {

  /**
   * This method reads the splits and make (blockPath, (min, max)) tuple to to min max pruning of
   * the src dataset
   * @param carbonTable                       target carbon table object
   * @param colToSplitsFilePathAndMinMaxMap   CarbonInputSplit whose min max cached in driver or
   *                                          the index server
   * @param joinColumnsToComparatorMap        This map contains the column to comparator mapping
   *                                          which will help in compare and update min max
   * @param fileMinMaxMapListOfAllJoinColumns collection to hold the filepath and min max of all the
   *                                          join columns involved
   */
  def addFilePathAndMinMaxTuples(
      colToSplitsFilePathAndMinMaxMap: mutable.Map[String, util.List[FilePathMinMaxVO]],
      carbonTable: CarbonTable,
      joinColumnsToComparatorMap: mutable.LinkedHashMap[CarbonColumn, SerializableComparator],
      fileMinMaxMapListOfAllJoinColumns: mutable.ArrayBuffer[(mutable.Map[String, (AnyRef, AnyRef)],
        CarbonColumn)]): Unit = {
    joinColumnsToComparatorMap.foreach { case (joinColumn, comparator) =>
      val fileMinMaxMap: mutable.Map[String, (AnyRef, AnyRef)] =
        collection.mutable.Map.empty[String, (AnyRef, AnyRef)]
      val joinDataType = joinColumn.getDataType
      val isDimension = joinColumn.isDimension
      val isPrimitiveAndNotDate = DataTypeUtil.isPrimitiveColumn(joinDataType) &&
                                  (joinDataType != DataTypes.DATE)
      colToSplitsFilePathAndMinMaxMap(joinColumn.getColName).asScala.foreach {
        filePathMinMiax =>
          val filePath = filePathMinMiax.getFilePath
          val minBytes = filePathMinMiax.getMin
          val maxBytes = filePathMinMiax.getMax
          val uniqBlockPath = if (carbonTable.isHivePartitionTable) {
            // While data loading to SI created on Partition table, on
            // partition directory, '/' will be
            // replaced with '#', to support multi level partitioning. For example, BlockId will be
            // look like `part1=1#part2=2/xxxxxxxxx`. During query also, blockId should be
            // replaced by '#' in place of '/', to match and prune data on SI table.
            CarbonUtil.getBlockId(carbonTable.getAbsoluteTableIdentifier,
              filePath,
              "",
              true,
              false,
              true)
          } else {
            filePath.substring(filePath.lastIndexOf("/Part") + 1)
          }
          if (isDimension) {
            if (isPrimitiveAndNotDate) {
              val minValue = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(minBytes,
                joinDataType)
              val maxValue = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(maxBytes,
                joinDataType)
              // check here if present in map, if it is, compare and update min and amx
              if (fileMinMaxMap.contains(uniqBlockPath)) {
                val isMinLessThanMin =
                  comparator.compare(fileMinMaxMap(uniqBlockPath)._1, minValue) > 0
                val isMaxMoreThanMax =
                  comparator.compare(maxValue, fileMinMaxMap(uniqBlockPath)._2) > 0
                updateMapIfRequiredBasedOnMinMax(fileMinMaxMap,
                  minValue,
                  maxValue,
                  uniqBlockPath,
                  isMinLessThanMin,
                  isMaxMoreThanMax)
              } else {
                fileMinMaxMap += (uniqBlockPath -> (minValue, maxValue))
              }
            } else {
              if (fileMinMaxMap.contains(uniqBlockPath)) {
                val isMinLessThanMin = ByteUtil.UnsafeComparer.INSTANCE
                                         .compareTo(fileMinMaxMap(uniqBlockPath)._1
                                           .asInstanceOf[String].getBytes(), minBytes) > 0
                val isMaxMoreThanMax = ByteUtil.UnsafeComparer.INSTANCE
                                         .compareTo(maxBytes, fileMinMaxMap(uniqBlockPath)._2
                                           .asInstanceOf[String].getBytes()) > 0
                updateMapIfRequiredBasedOnMinMax(fileMinMaxMap,
                  new String(minBytes),
                  new String(maxBytes),
                  uniqBlockPath,
                  isMinLessThanMin,
                  isMaxMoreThanMax)
              } else {
                fileMinMaxMap += (uniqBlockPath -> (new String(minBytes), new String(maxBytes)))
              }
            }
          } else {
            val maxValue = DataTypeUtil.getMeasureObjectFromDataType(maxBytes, joinDataType)
            val minValue = DataTypeUtil.getMeasureObjectFromDataType(minBytes, joinDataType)
            if (fileMinMaxMap.contains(uniqBlockPath)) {
              val isMinLessThanMin =
                comparator.compare(fileMinMaxMap(uniqBlockPath)._1, minValue) > 0
              val isMaxMoreThanMin =
                comparator.compare(maxValue, fileMinMaxMap(uniqBlockPath)._2) > 0
              updateMapIfRequiredBasedOnMinMax(fileMinMaxMap,
                minValue,
                maxValue,
                uniqBlockPath,
                isMinLessThanMin,
                isMaxMoreThanMin)
            } else {
              fileMinMaxMap += (uniqBlockPath -> (minValue, maxValue))
            }
          }
      }
      fileMinMaxMapListOfAllJoinColumns += ((fileMinMaxMap, joinColumn))
    }
  }

  /**
   * This method updates the min max map of the block if the value is less than min or more
   * than max
   */
  private def updateMapIfRequiredBasedOnMinMax(
      fileMinMaxMap: mutable.Map[String, (AnyRef, AnyRef)],
      minValue: AnyRef,
      maxValue: AnyRef,
      uniqBlockPath: String,
      isMinLessThanMin: Boolean,
      isMaxMoreThanMin: Boolean): Unit = {
    (isMinLessThanMin, isMaxMoreThanMin) match {
      case (true, true) => fileMinMaxMap(uniqBlockPath) = (minValue, maxValue)
      case (true, false) => fileMinMaxMap(uniqBlockPath) = (minValue,
        fileMinMaxMap(uniqBlockPath)._2)
      case (false, true) => fileMinMaxMap(uniqBlockPath) = (fileMinMaxMap(uniqBlockPath)._1,
        maxValue)
      case _ =>
    }
  }

  /**
   * This method returns the partitions required to scan in the target table based on the
   * partitions present in the src table or dataset
   */
  def getPartitionSpecToConsiderForPruning(
      sparkSession: SparkSession,
      srcCarbonTable: CarbonTable,
      targetCarbonTable: CarbonTable,
      identifier: TableIdentifier = null): util.List[PartitionSpec] = {
    val partitionsToConsider = if (targetCarbonTable.isHivePartitionTable) {
      // handle the case of multiple partition columns in src and target and subset of
      //  partition columns
      val srcTableIdentifier = if (identifier == null) {
        TableIdentifier(srcCarbonTable.getTableName, Some(srcCarbonTable.getDatabaseName))
      } else {
        identifier
      }
      val srcPartitions = CarbonFilters.getPartitions(
        Seq.empty,
        sparkSession,
        srcTableIdentifier)
        .map(_.toList.flatMap(_.getPartitions.asScala))
        .orNull
      // get all the partitionSpec of target table which intersects with source partitions
      // example if the target has partitions as a=1/b=2/c=3, and src has e=1/a=1/b=2/d=4
      // we will consider the specific target partition as intersect gives results and also we
      // don't want to go very fine grain for partitions as location will be a single for nested
      // partitions.
      CarbonFilters.getPartitions(
        Seq.empty,
        sparkSession,
        TableIdentifier(
          targetCarbonTable.getTableName,
          Some(targetCarbonTable.getDatabaseName))).map(_.toList.filter {
        partitionSpec =>
          partitionSpec.getPartitions.asScala.intersect(srcPartitions).nonEmpty
      }).orNull
    } else {
      null
    }
    partitionsToConsider.asJava
  }
}
