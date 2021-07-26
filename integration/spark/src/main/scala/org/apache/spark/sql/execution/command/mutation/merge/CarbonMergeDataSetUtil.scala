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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.execution.CastExpressionOptimization
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.DateType

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.{IndexChooser, IndexInputFormat, IndexStoreManager, IndexUtil}
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexRowIndexes
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.mutate.{CdcVO, FilePathMinMaxVO}
import org.apache.carbondata.core.range.BlockMinMaxTree
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.comparator.SerializableComparator
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.spark.util.CarbonSparkUtil

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

  /**
   * This method get the files to scan by searching the tree prepared for each column with its
   * min-max
   * @param joinCarbonColumns join carbon columns
   * @param joinColumnToTreeMapping mapping of join column to interval tree
   * @param repartitionedSrcDs source dataset
   * @return carbondata files required to scan
   */
  def getFilesToScan(
      joinCarbonColumns: mutable.Set[CarbonColumn],
      joinColumnToTreeMapping: mutable.LinkedHashMap[CarbonColumn, BlockMinMaxTree],
      repartitionedSrcDs: Dataset[Row]): Array[String] = {
    var finalCarbonFilesToScan: Array[String] = Array.empty[String]
    val timeStampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    joinCarbonColumns.foreach { joinColumn =>
      val srcDeduplicatedRDD = repartitionedSrcDs.select(joinColumn.getColName).rdd
      finalCarbonFilesToScan ++= srcDeduplicatedRDD.mapPartitions { iter =>
        val filesPerTask = new util.HashSet[String]()
        new Iterator[util.HashSet[String]] {
          override def hasNext: Boolean = {
            iter.hasNext
          }

          override def next(): util.HashSet[String] = {
            val row = iter.next()
            joinColumnToTreeMapping
              .foreach { joinColumnWithRangeTree =>
                val joinCarbonColumn = joinColumnWithRangeTree._1
                val rangeIntervalTree = joinColumnWithRangeTree._2
                val joinDataType = joinCarbonColumn.getDataType
                val isDimension = joinCarbonColumn.isDimension
                val isPrimitiveAndNotDate = DataTypeUtil.isPrimitiveColumn(joinDataType) &&
                                            (joinDataType != DataTypes.DATE)
                val fieldIndex = row.fieldIndex(joinCarbonColumn.getColName)
                val fieldValue = if (!row.isNullAt(fieldIndex)) {
                  if (isDimension) {
                    if (joinDataType != DataTypes.DATE) {
                      DataTypeUtil.getBytesBasedOnDataTypeForNoDictionaryColumn(row
                        .getAs(fieldIndex)
                        .toString,
                        joinDataType, timeStampFormat)
                    } else {
                      // if date, then get the key from direct dict generator and then get bytes
                      val actualValue = row.getAs(fieldIndex)
                      val dateSurrogateValue = CastExpressionOptimization
                        .typeCastStringToLong(actualValue, DateType).asInstanceOf[Int]
                      ByteUtil.convertIntToBytes(dateSurrogateValue)
                    }
                  } else {
                    CarbonUtil.getValueAsBytes(joinDataType, row.getAs(fieldIndex))
                  }
                } else {
                  // here handling for null values
                  val value: Long = 0
                  if (isDimension) {
                    if (isPrimitiveAndNotDate) {
                      CarbonCommonConstants.EMPTY_BYTE_ARRAY
                    } else {
                      CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY
                    }
                  } else {
                    val nullValueForMeasure = if ((joinDataType eq DataTypes.BOOLEAN) ||
                                                  (joinDataType eq DataTypes.BYTE)) {
                      value.toByte
                    } else if (joinDataType eq DataTypes.SHORT) {
                      value.toShort
                    } else if (joinDataType eq DataTypes.INT) {
                      value.toInt
                    } else if ((joinDataType eq DataTypes.LONG) ||
                               (joinDataType eq DataTypes.TIMESTAMP)) {
                      value
                    } else if (joinDataType eq DataTypes.DOUBLE) {
                      0d
                    } else if (joinDataType eq DataTypes.FLOAT) {
                      0f
                    } else if (DataTypes.isDecimal(joinDataType)) {
                      value
                    }
                    CarbonUtil.getValueAsBytes(joinDataType, nullValueForMeasure)
                  }
                }
                rangeIntervalTree.getMatchingFiles(fieldValue, filesPerTask)
              }
            filesPerTask
          }
        }
      }.flatMap(_.asScala.toList).map(filePath => (filePath, 0)).reduceByKey((m, n) => m + n)
        .collect().map(_._1)
    }
    finalCarbonFilesToScan
  }

  /**
   * This method gets the key columnsrequired for joining based on merge condition
   * @param keyColumn key columns if specified in the upsert APIs
   * @param targetDsAliasName target table alias name
   * @param targetCarbonTable target carbon table
   * @param mergeMatches merge match conditions from the user
   * @return set of key columns required to be used in pruning
   */
  def getTargetTableKeyColumns(
      keyColumn: String,
      targetDsAliasName: String,
      targetCarbonTable: CarbonTable,
      mergeMatches: MergeDataSetMatches): mutable.Set[String] = {
    var targetKeyColumns: mutable.Set[String] = mutable.Set.empty[String]
    if (mergeMatches != null) {
      mergeMatches.joinExpr.expr.collect {
        case EqualTo(left, right) =>
          left match {
            case attribute: UnresolvedAttribute if right.isInstanceOf[UnresolvedAttribute] =>
              val leftAlias = attribute.nameParts.head
              if (targetDsAliasName != null) {
                if (targetDsAliasName.equalsIgnoreCase(leftAlias)) {
                  targetKeyColumns += attribute.nameParts.tail.head
                } else {
                  targetKeyColumns +=
                  right.asInstanceOf[UnresolvedAttribute].nameParts.tail.head
                }
              } else {
                if (leftAlias.equalsIgnoreCase(targetCarbonTable.getTableName)) {
                  targetKeyColumns += attribute.nameParts.tail.head
                } else {
                  targetKeyColumns +=
                  right.asInstanceOf[UnresolvedAttribute].nameParts.tail.head
                }
              }
            case _ =>
          }
      }
      targetKeyColumns
    } else {
      targetKeyColumns += keyColumn
    }
  }

  /**
   * This method get the blocklets and cache either in driver or index server and returns column
   * FilePathMinMaxVO object
   * @param targetCarbonTable target carbondata table
   * @param repartitionedSrcDs source dataset
   * @param columnMinMaxInBlocklet mapping of column to it's min max in each blocklet
   * @param columnToIndexMap mapping of column to its index to fetch from the index row
   * @param sparkSession spark session
   * @return mapping of column to FilePathMinMaxVO object which contains filepath and min, max
   */
  def getSplitsAndLoadToCache(
      targetCarbonTable: CarbonTable,
      repartitionedSrcDs: Dataset[Row],
      columnMinMaxInBlocklet: util.LinkedHashMap[String, util.List[FilePathMinMaxVO]],
      columnToIndexMap: util.Map[String, Integer],
      sparkSession: SparkSession): mutable.Map[String, util.List[FilePathMinMaxVO]] = {
    val isDistributedPruningEnabled: Boolean = CarbonProperties.getInstance
      .isDistributedPruningEnabled(targetCarbonTable.getDatabaseName,
        targetCarbonTable.getTableName)
    // if the index server is enabled, call index server to cache the index and get all the
    // blocklets of the target table. If the index server disabled, just call the getSplits of
    // the driver side to cache and get the splits. These CarbonInputSplits basically contain
    // the filePaths and the min max of each columns.
    val ssm = new SegmentStatusManager(targetCarbonTable.getAbsoluteTableIdentifier)
    val validSegments = ssm.getValidAndInvalidSegments.getValidSegments
    val defaultIndex = IndexStoreManager.getInstance.getDefaultIndex(targetCarbonTable)

    // 1. identify if src is partition table, if both src and target target for partition table
    // on same column(s), then only get the src partitions and send those partitions to scan in
    // target handling only for carbon src dataset now
    val srcDataSetRelations = CarbonSparkUtil.collectCarbonRelation(repartitionedSrcDs.logicalPlan)
    val partitionsToConsider =
      if (srcDataSetRelations.lengthCompare(1) == 0 &&
          srcDataSetRelations.head.isInstanceOf[CarbonDatasourceHadoopRelation]) {
        val srcCarbonTable = srcDataSetRelations.head.carbonRelation.carbonTable
        if (srcCarbonTable.isHivePartitionTable) {
          CarbonMergeDataSetUtil.getPartitionSpecToConsiderForPruning(
            sparkSession,
            srcCarbonTable,
            targetCarbonTable)
        } else {
          null
        }
      } else {
        val nonCarbonRelations = CarbonSparkUtil.collectNonCarbonRelation(repartitionedSrcDs
          .logicalPlan)
        // when the relations are not empty, it means the source dataset is prepared from table
        if (nonCarbonRelations.nonEmpty &&
            nonCarbonRelations.head.catalogTable.isDefined &&
            nonCarbonRelations.head.catalogTable.get.partitionColumnNames != null) {
          CarbonMergeDataSetUtil.getPartitionSpecToConsiderForPruning(
            sparkSession,
            null,
            targetCarbonTable,
            nonCarbonRelations.head.catalogTable.get.identifier)
        } else {
          null
        }
      }

    if (isDistributedPruningEnabled) {
      val indexFormat = new IndexInputFormat(targetCarbonTable, null, validSegments,
        Nil.asJava, partitionsToConsider, false, null, false, false)
      val cdcVO = new CdcVO(columnToIndexMap)
      indexFormat.setCdcVO(cdcVO)
      IndexServer.getClient.getSplits(indexFormat).getExtendedBlocklets(indexFormat).asScala
        .flatMap { blocklet =>
          blocklet.getColumnToMinMaxMapping.asScala.map {
            case (columnName, minMaxListWithFilePath) =>
              val filePathMinMaxList = columnMinMaxInBlocklet.get(columnName)
              if (filePathMinMaxList != null) {
                filePathMinMaxList.addAll(minMaxListWithFilePath)
                columnMinMaxInBlocklet.put(columnName, filePathMinMaxList)
              } else {
                columnMinMaxInBlocklet.put(columnName, minMaxListWithFilePath)
              }
          }
        }
      columnMinMaxInBlocklet.asScala
    } else {
      if (targetCarbonTable.isTransactionalTable) {
        val indexExprWrapper = IndexChooser.getDefaultIndex(targetCarbonTable, null)
        IndexUtil.loadIndexes(targetCarbonTable, indexExprWrapper, validSegments)
      }
      val blocklets = defaultIndex.prune(validSegments, null, partitionsToConsider).asScala
      columnToIndexMap.asScala.foreach {
        case (columnName, index) =>
          val filePathAndMinMaxList = new util.ArrayList[FilePathMinMaxVO]()
          blocklets.map { blocklet =>
            val filePathMinMax = new FilePathMinMaxVO(blocklet.getFilePath,
              CarbonUtil.getMinMaxValue(blocklet
                .getInputSplit
                .getIndexRow,
                BlockletIndexRowIndexes.MIN_VALUES_INDEX)(index),
              CarbonUtil.getMinMaxValue(blocklet
                .getInputSplit
                .getIndexRow,
                BlockletIndexRowIndexes.MAX_VALUES_INDEX)(index))
            filePathAndMinMaxList.add(filePathMinMax)
          }
          columnMinMaxInBlocklet.put(columnName, filePathAndMinMaxList)
      }
      columnMinMaxInBlocklet.asScala
    }
  }
}
