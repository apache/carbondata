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

import java.nio.charset.Charset
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.execution.CastExpressionOptimization
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel, AlterTableDropColumnModel}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}
import org.apache.spark.sql.execution.strategy.DDLHelper
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.{DateType, DecimalType, StructField}

import org.apache.carbondata.common.exceptions.sql.CarbonSchemaException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.{IndexChooser, IndexInputFormat, IndexStoreManager, IndexUtil}
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexRowIndexes
import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.mutate.{CdcVO, FilePathMinMaxVO}
import org.apache.carbondata.core.range.BlockMinMaxTree
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.comparator.{Comparator, SerializableComparator}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * The utility class for Merge operations
 */
object CarbonMergeDataSetUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

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

  /**
   * This method verifies source and target schemas for the following:
   * If additional columns are present in source schema as compared to target, simply ignore them.
   * If some columns are missing in source schema as compared to target schema, exception is thrown.
   * If data type of some column differs in source and target schemas, exception is thrown.
   * If source schema has multiple columns whose names differ only in case sensitivity, exception
   * is thrown.
   *
   * @param targetDs target carbondata table
   * @param srcDs    source/incoming data
   */
  def verifySourceAndTargetSchemas(targetDs: Dataset[Row], srcDs: Dataset[Row]): Unit = {
    LOGGER.info("schema enforcement is enabled. Source and target schemas will be verified")
    // get the source and target dataset schema
    val sourceSchema = srcDs.schema
    val targetSchema = targetDs.schema

    verifyBackwardsCompatibility(targetDs, srcDs)

    val lowerCaseSrcSchemaFields = sourceSchema.fields.map(_.name.toLowerCase)

    // check if some additional column got added in source schema
    if (sourceSchema.fields.length > targetSchema.fields.length) {
      val tgtSchemaInLowerCase = targetSchema.fields.map(_.name.toLowerCase)
      val additionalSourceFields = lowerCaseSrcSchemaFields
        .filterNot(srcField => {
          tgtSchemaInLowerCase.contains(srcField)
        })
      if (additionalSourceFields.nonEmpty) {
        LOGGER.warn(s"source schema contains additional fields which are not present in " +
                    s"target schema: ${ additionalSourceFields.mkString(",") }")
      }
    }
  }

  def verifyCaseSensitiveFieldNames(
      lowerCaseSrcSchemaFields: Array[String]
  ): Unit = {
    // check if source schema has fields whose names only differ in case sensitivity
    val similarFields = lowerCaseSrcSchemaFields.groupBy(a => identity(a)).map {
      case (str, times) => (str, times.length)
    }.toList.filter(e => e._2 > 1).map(_._1)
    if (similarFields.nonEmpty) {
      val errorMsg = s"source schema has similar fields which differ only in case sensitivity: " +
                     s"${ similarFields.mkString(",") }"
      LOGGER.error(errorMsg)
      throw new CarbonSchemaException(errorMsg)
    }
  }

  /**
   * This method takes care of handling schema evolution scenarios for CarbonStreamer class.
   * Currently only addition of columns is supported.
   *
   * @param targetDs     target dataset whose schema needs to be modified, if applicable
   * @param srcDs        incoming dataset
   * @param sparkSession SparkSession
   */
  def handleSchemaEvolutionForCarbonStreamer(targetDs: Dataset[Row], srcDs: Dataset[Row],
      sparkSession: SparkSession): Unit = {
    // read the property here
    val isSchemaEnforcementEnabled = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_ENABLE_SCHEMA_ENFORCEMENT,
        CarbonCommonConstants.CARBON_ENABLE_SCHEMA_ENFORCEMENT_DEFAULT).toBoolean
    if (isSchemaEnforcementEnabled) {
      verifySourceAndTargetSchemas(targetDs, srcDs)
    } else {
      // These meta columns should be removed before actually writing the data
      val metaColumnsString = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_STREAMER_META_COLUMNS, "")
      val metaCols = metaColumnsString.split(",").map(_.trim)
      val srcDsWithoutMeta = if (metaCols.length > 0) {
        srcDs.drop(metaCols: _*)
      } else {
        srcDs
      }
      handleSchemaEvolution(targetDs, srcDsWithoutMeta, sparkSession, isStreamerInvolved = true)
    }
  }

  def verifyBackwardsCompatibility(
      targetDs: Dataset[Row],
      srcDs: Dataset[Row]): Unit = {
    val sourceSchema = srcDs.schema
    val targetSchema = targetDs.schema

    targetSchema.fields.foreach(tgtField => {
      val sourceField = sourceSchema.fields
        .find(f => f.name.equalsIgnoreCase(tgtField.name))
      // check if some field is missing in source schema
      if (sourceField.isEmpty) {
        val errorMsg = s"source schema does not contain field: ${ tgtField.name }"
        LOGGER.error(errorMsg)
        throw new CarbonSchemaException(errorMsg)
      }

      // check if data type got modified for some column
      if (!sourceField.get.dataType.equals(tgtField.dataType)) {
        val errorMsg = s"source schema has different data type " +
                       s"for field: ${ tgtField.name }"
        LOGGER.error(errorMsg + s", source type: ${ sourceField.get.dataType }, " +
                     s"target type: ${ tgtField.dataType }")
        throw new CarbonSchemaException(errorMsg)
      }
    })

    val lowerCaseSrcSchemaFields = sourceSchema.fields.map(_.name.toLowerCase)
    verifyCaseSensitiveFieldNames(lowerCaseSrcSchemaFields)
  }

  /**
   * The method takes care of following schema evolution cases:
   * Addition of a new column in source schema which is not present in target
   * Deletion of a column in source schema which is present in target
   * Data type changes for an existing column.
   * The method does not take care of column renames and table renames
   *
   * @param targetDs existing target dataset
   * @param srcDs    incoming source dataset
   * @return new target schema to write the incoming batch with
   */
  def handleSchemaEvolution(
      targetDs: Dataset[Row],
      srcDs: Dataset[Row],
      sparkSession: SparkSession,
      isStreamerInvolved: Boolean = false): Unit = {

    /*
    If the method is called from CarbonStreamer, we need to ensure the schema is evolved in
    backwards compatible way. In phase 1, only addition of columns is supported, hence this check is
    needed to ensure data integrity.
    The existing IUD flow supports full schema evolution, hence this check is not needed for
     existing flows.
     */
    if (isStreamerInvolved) {
      verifyBackwardsCompatibility(targetDs, srcDs)
    }
    val sourceSchema = srcDs.schema
    val targetSchema = targetDs.schema

    // check if any column got added in source
    val addedColumns = sourceSchema.fields
      .filterNot(field => targetSchema.fields.map(_.name).contains(field.name))
    val relations = CarbonSparkUtil.collectCarbonRelation(targetDs.logicalPlan)
    val targetCarbonTable = relations.head.carbonRelation.carbonTable
    if (addedColumns.nonEmpty) {
      handleAddColumnScenario(targetDs,
        sourceSchema.fields.filter(f => addedColumns.contains(f)).toSeq,
        sparkSession,
        targetCarbonTable)
    }

    // check if any column got deleted from source
    val partitionInfo = targetCarbonTable.getPartitionInfo
    val partitionColumns = if (partitionInfo != null) {
      partitionInfo.getColumnSchemaList.asScala
        .map(_.getColumnName).toList
    } else {
      List[String]()
    }
    val srcSchemaFieldsInLowerCase = sourceSchema.fields.map(_.name.toLowerCase)
    val deletedColumns = targetSchema.fields.map(_.name.toLowerCase)
      .filterNot(f => {
        srcSchemaFieldsInLowerCase.contains(f) ||
        partitionColumns.contains(f)
      })
    if (deletedColumns.nonEmpty) {
      handleDeleteColumnScenario(targetDs, deletedColumns.toList, sparkSession, targetCarbonTable)
    }

    val modifiedColumns = targetSchema.fields.filter(tgtField => {
      val sourceField = sourceSchema.fields.find(f => f.name.equalsIgnoreCase(tgtField.name))
      if (sourceField.isDefined) {
        !sourceField.get.dataType.equals(tgtField.dataType)
      } else {
        false
      }
    })

    if (modifiedColumns.nonEmpty) {
      handleDataTypeChangeScenario(targetDs,
        modifiedColumns.toList,
        sparkSession,
        targetCarbonTable)
    }
  }

  /**
   * This method calls CarbonAlterTableAddColumnCommand for adding new columns
   *
   * @param targetDs     target dataset whose schema needs to be modified
   * @param colsToAdd    new columns to be added
   * @param sparkSession SparkSession
   */
  def handleAddColumnScenario(targetDs: Dataset[Row], colsToAdd: Seq[StructField],
      sparkSession: SparkSession,
      targetCarbonTable: CarbonTable): Unit = {
    val alterTableAddColsCmd = DDLHelper.prepareAlterTableAddColsCommand(
      Option(targetCarbonTable.getDatabaseName),
      colsToAdd,
      targetCarbonTable.getTableName.toLowerCase)
    alterTableAddColsCmd.run(sparkSession)
  }

  /**
   * This method calls CarbonAlterTableDropColumnCommand for deleting columns
   *
   * @param targetDs     target dataset whose schema needs to be modified
   * @param colsToDrop   columns to be dropped from carbondata table
   * @param sparkSession SparkSession
   */
  def handleDeleteColumnScenario(targetDs: Dataset[Row], colsToDrop: List[String],
      sparkSession: SparkSession,
      targetCarbonTable: CarbonTable): Unit = {
    val alterTableDropColumnModel = AlterTableDropColumnModel(
      CarbonParserUtil.convertDbNameToLowerCase(Option(targetCarbonTable.getDatabaseName)),
      targetCarbonTable.getTableName.toLowerCase,
      colsToDrop.map(_.toLowerCase))
    CarbonAlterTableDropColumnCommand(alterTableDropColumnModel).run(sparkSession)
  }

  /**
   * This method calls CarbonAlterTableColRenameDataTypeChangeCommand for handling data type changes
   *
   * @param targetDs     target dataset whose schema needs to be modified
   * @param modifiedCols columns with data type changes
   * @param sparkSession SparkSession
   */
  def handleDataTypeChangeScenario(targetDs: Dataset[Row], modifiedCols: List[StructField],
      sparkSession: SparkSession,
      targetCarbonTable: CarbonTable): Unit = {
    // need to call the command one by one for each modified column
    modifiedCols.foreach(col => {
      val alterTableColRenameDataTypeChangeCommand = DDLHelper
        .prepareAlterTableColRenameDataTypeChangeCommand(
          col,
          Option(targetCarbonTable.getDatabaseName.toLowerCase),
          targetCarbonTable.getTableName.toLowerCase,
          col.name.toLowerCase,
          isColumnRename = false,
          Option.empty)
      alterTableColRenameDataTypeChangeCommand.run(sparkSession)
    })
  }

  def deduplicateBeforeWriting(
      srcDs: Dataset[Row],
      targetDs: Dataset[Row],
      sparkSession: SparkSession,
      srcAlias: String,
      targetAlias: String,
      keyColumn: String,
      orderingField: String,
      targetCarbonTable: CarbonTable): Dataset[Row] = {
    val properties = CarbonProperties.getInstance()
    val filterDupes = properties
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_INSERT_DEDUPLICATE,
        CarbonCommonConstants.CARBON_STREAMER_INSERT_DEDUPLICATE_DEFAULT).toBoolean
    val combineBeforeUpsert = properties
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_UPSERT_DEDUPLICATE,
        CarbonCommonConstants.CARBON_STREAMER_UPSERT_DEDUPLICATE_DEFAULT).toBoolean
    var dedupedDataset: Dataset[Row] = srcDs
    if (combineBeforeUpsert) {
      dedupedDataset = deduplicateAgainstIncomingDataset(srcDs, sparkSession, srcAlias, keyColumn,
        orderingField, targetCarbonTable)
    }
    if (filterDupes) {
      dedupedDataset = deduplicateAgainstExistingDataset(dedupedDataset, targetDs,
        srcAlias, targetAlias, keyColumn)
    }
    dedupedDataset
  }

  def deduplicateAgainstIncomingDataset(
      srcDs: Dataset[Row],
      sparkSession: SparkSession,
      srcAlias: String,
      keyColumn: String,
      orderingField: String,
      table: CarbonTable): Dataset[Row] = {
    if (orderingField.equals(CarbonCommonConstants.CARBON_STREAMER_SOURCE_ORDERING_FIELD_DEFAULT)) {
      return srcDs
    }
    val schema = srcDs.schema
    val carbonKeyColumn = table.getColumnByName(keyColumn)
    val keyColumnDataType = getCarbonDataType(keyColumn, srcDs)
    val orderingFieldDataType = getCarbonDataType(orderingField, srcDs)
    val isPrimitiveAndNotDate = DataTypeUtil.isPrimitiveColumn(orderingFieldDataType) &&
                                (orderingFieldDataType != DataTypes.DATE)
    val comparator = Comparator.getComparator(orderingFieldDataType)
    val rdd = srcDs.rdd
    val dedupedRDD: RDD[Row] = rdd.map { row =>
      val index = row.fieldIndex(keyColumn)
      val rowKey = getRowKey(row, index, carbonKeyColumn, isPrimitiveAndNotDate, keyColumnDataType)
      (rowKey, row)
    }.reduceByKey { (row1, row2) =>
      var orderingValue1 = row1.getAs(orderingField).asInstanceOf[Any]
      var orderingValue2 = row2.getAs(orderingField).asInstanceOf[Any]
      if (orderingValue1 == null) {
        row2
      } else if (orderingValue2 == null) {
        row1
      } else {
        if (orderingFieldDataType.equals(DataTypes.STRING)) {
          orderingValue1 = orderingValue1.toString
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))
          orderingValue2 = orderingValue2.toString
            .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))
        }
        if (comparator.compare(orderingValue1, orderingValue2) >= 0) {
          row1
        } else {
          row2
        }
      }
    }.map(_._2)
    sparkSession.createDataFrame(dedupedRDD, schema).alias(srcAlias)
  }

  def getRowKey(
      row: Row,
      index: Integer,
      carbonKeyColumn: CarbonColumn,
      isPrimitiveAndNotDate: Boolean,
      keyColumnDataType: CarbonDataType
  ): AnyRef = {
    if (!row.isNullAt(index)) {
      row.getAs(index).toString
    } else {
      val value: Long = 0
      if (carbonKeyColumn.isDimension) {
        if (isPrimitiveAndNotDate) {
          CarbonCommonConstants.EMPTY_BYTE_ARRAY
        } else {
          CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY
        }
      } else {
        val nullValueForMeasure = keyColumnDataType match {
          case DataTypes.BOOLEAN | DataTypes.BYTE => value.toByte
          case DataTypes.SHORT => value.toShort
          case DataTypes.INT => value.toInt
          case DataTypes.DOUBLE => 0d
          case DataTypes.FLOAT => 0f
          case DataTypes.LONG | DataTypes.TIMESTAMP => value
          case _ => value
        }
        CarbonUtil.getValueAsBytes(keyColumnDataType, nullValueForMeasure)
      }
    }
  }

  def getCarbonDataType(
      fieldName: String,
      srcDs: Dataset[Row]
  ): CarbonDataType = {
    val schema = srcDs.schema
    val dataType = schema.fields.find(f => f.name.equalsIgnoreCase(fieldName)).get.dataType
    CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType)
  }

  def deduplicateAgainstExistingDataset(
      srcDs: Dataset[Row],
      targetDs: Dataset[Row],
      srcAlias: String,
      targetAlias: String,
      keyColumn: String
  ): Dataset[Row] = {
    srcDs.join(targetDs,
      expr(s"$srcAlias.$keyColumn = $targetAlias.$keyColumn"), "left_anti")
  }
}
