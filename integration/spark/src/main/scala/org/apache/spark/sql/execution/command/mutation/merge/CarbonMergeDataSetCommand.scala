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
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, CarbonToSparkAdapter, Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.avro.AvroFileFormatFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.{CastExpressionOptimization, LogicalRDD, ProjectExec}
import org.apache.spark.sql.execution.command.{DataCommand, ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.mutation.HorizontalCompaction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, LongAccumulator}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexChooser, IndexInputFormat, IndexStoreManager, IndexUtil}
import org.apache.carbondata.core.indexstore.blockletindex.{BlockIndex, BlockletIndexRowIndexes}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.mutate.{CdcVO, FilePathMinMaxVO}
import org.apache.carbondata.core.range.{BlockMinMaxTree, MinMaxNode}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.comparator.{Comparator, SerializableComparator}
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * This command will merge the data of source dataset to target dataset backed by carbon table.
 * @param targetDsOri Target dataset to merge the data. It should be backed by carbon table
 * @param srcDS  Source dataset, it can be any data.
 * @param mergeMatches It contains the join condition and list match conditions to apply.
 */
case class CarbonMergeDataSetCommand(
    targetDsOri: Dataset[Row],
    srcDS: Dataset[Row],
    var mergeMatches: MergeDataSetMatches = null,
    keyColumn: String = null,
    operationType: String = null)
  extends DataCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  private val status_on_mergeds = "status_on_mergeds"

  /**
   * It merge the data of source dataset to target dataset backed by carbon table. Basically it
   * makes the full outer join with both source and target and apply the given conditions as "case
   * when" to get the status to process the row. The status could be insert/update/delete.
   * It also can insert the history(update/delete) data to history table.
   *
   */
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val relations = CarbonSparkUtil.collectCarbonRelation(targetDsOri.logicalPlan)
    val st = System.currentTimeMillis()
    val targetDsAliasName = targetDsOri.logicalPlan match {
      case alias: SubqueryAlias =>
        alias.alias
      case _ => null
    }
    val sourceAliasName = srcDS.logicalPlan match {
      case alias: SubqueryAlias =>
        alias.alias
      case _ => null
    }
    if (relations.length != 1) {
      throw new UnsupportedOperationException(
        "Carbon table supposed to be present in merge dataset")
    }
    // Target dataset must be backed by carbondata table.
    val targetCarbonTable = relations.head.carbonRelation.carbonTable
    // select only the required columns, it can avoid lot of and shuffling.
    val targetDs = if (mergeMatches == null && operationType != null) {
      targetDsOri.select(keyColumn)
    } else {
      // Get all the required columns of targetDS by going through all match conditions and actions.
      val columns = getSelectExpressionsOnExistingDF(targetDsOri, mergeMatches, sparkSession)
      targetDsOri.select(columns: _*)
    }
    // decide join type based on match conditions or based on merge operation type
    val joinType = if (mergeMatches == null && operationType != null) {
      MergeOperationType.withName(operationType.toUpperCase) match {
        case MergeOperationType.UPDATE | MergeOperationType.DELETE =>
          "inner"
        case MergeOperationType.UPSERT =>
          "right_outer"
        case MergeOperationType.INSERT =>
          null
      }
    } else {
      decideJoinType
    }

    val joinColumns = if (mergeMatches == null) {
      Seq(keyColumn)
    } else {
      mergeMatches.joinExpr.expr.collect {
        case unresolvedAttribute: UnresolvedAttribute if unresolvedAttribute.nameParts.nonEmpty =>
          // Let's say the join condition will be something like A.id = B.id, then it will be an
          // EqualTo expression, with left expression as UnresolvedAttribute(A.id) and right will
          // be a Literal(B.id). Since we need the column name here, we can directly check the left
          // which is UnresolvedAttribute. We take nameparts from UnresolvedAttribute which is an
          // ArrayBuffer containing "A" and "id", since "id" is column name, we take
          // nameparts.tail.head which gives us "id" column name.
          unresolvedAttribute.nameParts.tail.head
      }.distinct
    }

    // repartition the srsDs, if the target has bucketing and the bucketing columns contains join
    // columns
    val repartitionedSrcDs =
      if (targetCarbonTable.getBucketingInfo != null &&
          targetCarbonTable.getBucketingInfo
            .getListOfColumns
            .asScala
            .map(_.getColumnName).containsSlice(joinColumns)) {
        srcDS.repartition(targetCarbonTable.getBucketingInfo.getNumOfRanges,
          joinColumns.map(srcDS.col): _*)
      } else {
      srcDS
      }

    // cache the source data as we will be scanning multiple times
    repartitionedSrcDs.cache()
    val deDuplicatedRecords = repartitionedSrcDs.count()
    LOGGER.info(s"Number of records from source data: $deDuplicatedRecords")
    // Create accumulators to log the stats
    val stats = Stats(createLongAccumulator("insertedRows"),
      createLongAccumulator("updatedRows"),
      createLongAccumulator("deletedRows"))
    // check if its just upsert/update/delete/insert operation and go to UpsertHandler
    var finalCarbonFilesToScan: Array[String] = Array.empty[String]
    // the pruning will happen when the join type is not full_outer, in case of full_outer,
    // we will be needing all the records from left table which is target table, so no need to prune
    // target table based on min max of source table.
    val isMinMaxPruningEnabled = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_CDC_MINMAX_PRUNING_ENABLED,
        CarbonCommonConstants.CARBON_CDC_MINMAX_PRUNING_ENABLED_DEFAULT).toBoolean
    var didNotPrune = false
    breakable {
      if (isMinMaxPruningEnabled && joinType != null && !joinType.equalsIgnoreCase("full_outer")) {
        // if the index server is enabled, call index server to cache the index and get all the
        // blocklets of the target table. If the index server disabled, just call the getSplits of
        // the driver side to cache and get the splits. These CarbonInputSplits basically contain
        // the filePaths and the min max of each columns.
        val ssm = new SegmentStatusManager(targetCarbonTable.getAbsoluteTableIdentifier)
        val isDistributedPruningEnabled: Boolean = CarbonProperties.getInstance
          .isDistributedPruningEnabled(targetCarbonTable.getDatabaseName,
            targetCarbonTable.getTableName)
        val validSegments = ssm.getValidAndInvalidSegments.getValidSegments
        val defaultIndex = IndexStoreManager.getInstance.getDefaultIndex(targetCarbonTable)
        // 1. identify if src is partition table, if both src and target target for partition table
        // on same column(s), then only get the src partitions and send those partitions to scan in
        // target handling only for carbon src dataset now
        val srcDataSetRelations = CarbonSparkUtil.collectCarbonRelation(srcDS.logicalPlan)
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
            val nonCarbonRelations = CarbonSparkUtil.collectNonCarbonRelation(srcDS.logicalPlan)
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

        // 1. get all the join columns of equal to conditions or equi joins
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
        } else {
          targetKeyColumns += keyColumn
        }
        val joinCarbonColumns = targetKeyColumns.collect {
          case column => targetCarbonTable.getColumnByName(column)
        }

        LOGGER
          .info(s"Key columns for join are: ${ joinCarbonColumns.map(_.getColName).mkString(",") }")

        var columnToIndexMap: util.Map[String, Integer] = new util.LinkedHashMap[String, Integer]
        // get the min max cache column and based on that determine the index to check in min-max
        // array or Index Row
        val minMaxColumns = targetCarbonTable.getMinMaxCachedColumnsInCreateOrder
        if (minMaxColumns.size() != 0) {
          if (minMaxColumns.size() ==
              targetCarbonTable.getTableInfo.getFactTable.getListOfColumns.size() ||
              minMaxColumns.size() == 1 && minMaxColumns.get(0).equalsIgnoreCase("All columns")) {
            joinCarbonColumns.foreach { column =>
              if (column.isDimension) {
                columnToIndexMap.put(column.getColName, column.getOrdinal)
              } else {
                columnToIndexMap.put(column.getColName,
                  targetCarbonTable.getVisibleDimensions.size() + column.getOrdinal)
              }
            }
          } else {
            // handing case where only some columns are present as cached columns and check if those
            // columns has the target key columns or join columns
            val joinColumnsPresentInMinMaxCacheCols = joinCarbonColumns.map(_.getColName)
              .intersect(minMaxColumns.asScala.toSet)
            if (joinColumnsPresentInMinMaxCacheCols.isEmpty ||
                joinColumnsPresentInMinMaxCacheCols.size == joinCarbonColumns.size) {
              // 1. if none of the join columns are present in cache columns, then all blocklets
              // will be selected, so pruning is not required
              // 2. when one of the columns is not present in cache columns, no need to prune, as it
              // may lead to wrong data due to different filter conditions like OR
              didNotPrune = true
              break()
            }
          }
        }

        var columnMinMaxInBlocklet: util.LinkedHashMap[String, util.List[FilePathMinMaxVO]] = null
        val colTosplitsFilePathAndMinMaxMap: mutable.Map[String, util.List[FilePathMinMaxVO]] =
          if (isDistributedPruningEnabled) {
            val indexFormat = new IndexInputFormat(targetCarbonTable, null, validSegments,
              Nil.asJava, partitionsToConsider, false, null, false, false)
            columnMinMaxInBlocklet = new util.LinkedHashMap[String, util.List[FilePathMinMaxVO]]
            val cdcVO = new CdcVO(columnMinMaxInBlocklet, columnToIndexMap)
            indexFormat.setCdcVO(cdcVO)
            IndexServer.getClient.getSplits(indexFormat)
              .getExtendedBlocklets(indexFormat.getCarbonTable.getTablePath, indexFormat.getQueryId,
                indexFormat.isCountStarJob, indexFormat.getCdcVO)
              .asScala
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
            columnMinMaxInBlocklet = new util.LinkedHashMap[String, util.List[FilePathMinMaxVO]]
            columnToIndexMap.asScala.foreach {
              case (columnName, index) =>
                val filePathAndMinMaxList = new util.ArrayList[FilePathMinMaxVO]()
                blocklets.map { blocklet =>
                  val filePathMinMax = new FilePathMinMaxVO(blocklet.getFilePath,
                    BlockIndex.getMinMaxValue(blocklet
                      .getInputSplit
                      .getIndexRow,
                      BlockletIndexRowIndexes.MIN_VALUES_INDEX)(index),
                    BlockIndex.getMinMaxValue(blocklet
                      .getInputSplit
                      .getIndexRow,
                      BlockletIndexRowIndexes.MAX_VALUES_INDEX)(index))
                  filePathAndMinMaxList.add(filePathMinMax)
                }
                columnMinMaxInBlocklet.put(columnName, filePathAndMinMaxList)
            }
            columnMinMaxInBlocklet.asScala
          }

        LOGGER.info("Finished getting splits from driver or index server")

        // 2. get the tuple of filepath, min, max of the columns required, the min max should be
        // converted to actual value based on the datatype logic collection to store only block and
        // block level min and max
        val fileMinMaxMapListOfAllJoinColumns: mutable.ArrayBuffer[(mutable.Map[String,
          (AnyRef, AnyRef)], CarbonColumn)] =
        mutable.ArrayBuffer.empty[(mutable.Map[String, (AnyRef, AnyRef)], CarbonColumn)]

        val joinColumnsToComparatorMap:
          mutable.LinkedHashMap[CarbonColumn, SerializableComparator] =
          mutable.LinkedHashMap.empty[CarbonColumn, SerializableComparator]
        joinCarbonColumns.map { joinColumn =>
          val joinDataType = joinColumn.getDataType
          val isPrimitiveAndNotDate = DataTypeUtil.isPrimitiveColumn(joinDataType) &&
                                      (joinDataType != DataTypes.DATE)
          val comparator = if (isPrimitiveAndNotDate) {
            Comparator.getComparator(joinDataType)
          } else if (joinDataType == DataTypes.STRING) {
            null
          } else {
            Comparator.getComparatorByDataTypeForMeasure(joinDataType)
          }
          joinColumnsToComparatorMap += (joinColumn -> comparator)
        }

        // 3. prepare (filepath, (min, max)) at a block level.
        CarbonMergeDataSetUtil.addFilePathAndMinMaxTuples(colTosplitsFilePathAndMinMaxMap,
          targetCarbonTable,
          joinColumnsToComparatorMap,
          fileMinMaxMapListOfAllJoinColumns)

        // 4. prepare mapping of column and a range tree based on filepath, min and max for that
        // column. Here assumption is join expression columns will be less in actual use case.
        // Basically a primary column
        val joinColumnToTreeMapping: mutable.LinkedHashMap[CarbonColumn, BlockMinMaxTree] =
        mutable.LinkedHashMap.empty[CarbonColumn, BlockMinMaxTree]
        fileMinMaxMapListOfAllJoinColumns.foreach { case (fileMinMaxMap, joinCarbonColumn) =>
          val joinDataType = joinCarbonColumn.getDataType
          val isDimension = joinCarbonColumn.isDimension
          val isPrimitiveAndNotDate = DataTypeUtil.isPrimitiveColumn(joinDataType) &&
                                      (joinDataType != DataTypes.DATE)
          val comparator = joinColumnsToComparatorMap(joinCarbonColumn)
          val rangeIntervalTree = new BlockMinMaxTree(isPrimitiveAndNotDate,
            isDimension, joinDataType, comparator)
          fileMinMaxMap.foreach { case (filePath, minMax) =>
            rangeIntervalTree.insert(new MinMaxNode(filePath, minMax._1, minMax._2))
          }
          joinColumnToTreeMapping += ((joinCarbonColumn, rangeIntervalTree))
        }

        // 5.from srcRDD, do map and then for each row search in min max tree prepared above and
        // find the file paths to scan.
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

        LOGGER.info(s"Finished min-max pruning. Carbondata files to scan during merge is: ${
          finalCarbonFilesToScan.length
        }")
      }
    }

    // check if its just upsert/update/delete/insert operation and go to UpsertHandler
    if (mergeMatches == null && operationType != null) {
      val isInsertOperation = operationType.equalsIgnoreCase(MergeOperationType.INSERT.toString)
      val frame = if (isMinMaxPruningEnabled && !didNotPrune) {
        // if min-max pruning is enabled then we need to add blockUDFs filter to scan only the
        // pruned carbondata files from target carbon table.
        if (!isInsertOperation) {
          targetDs
            .withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, expr("getTupleId()"))
            .where(s"block_paths('${finalCarbonFilesToScan.mkString(",")}')")
            .join(repartitionedSrcDs.select(keyColumn),
              expr(s"$targetDsAliasName.$keyColumn = $sourceAliasName.$keyColumn"),
              joinType)
        } else {
          null
        }
      } else {
        if (!isInsertOperation) {
          targetDs
            .withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, expr("getTupleId()"))
            .join(repartitionedSrcDs.select(keyColumn),
              expr(s"$targetDsAliasName.$keyColumn = $sourceAliasName.$keyColumn"),
              joinType)
        } else {
          null
        }
      }
      val mergeHandler: MergeHandler =
        MergeOperationType.withName(operationType.toUpperCase) match {
        case MergeOperationType.UPSERT =>
          UpsertHandler(sparkSession, frame, targetCarbonTable, stats, repartitionedSrcDs)
        case MergeOperationType.UPDATE =>
          UpdateHandler(sparkSession, frame, targetCarbonTable, stats, repartitionedSrcDs)
        case MergeOperationType.DELETE =>
          DeleteHandler(sparkSession, frame, targetCarbonTable, stats, repartitionedSrcDs)
        case MergeOperationType.INSERT =>
          InsertHandler(sparkSession, frame, targetCarbonTable, stats, repartitionedSrcDs)
        }

      // execute merge handler
      mergeHandler.handleMerge()
      LOGGER.info(
        " Time taken to merge data  :: " + (System.currentTimeMillis() - st))
      // clear the cached src
      repartitionedSrcDs.unpersist()
      return Seq()
    }
    // validate the merge matches and actions.
    validateMergeActions(mergeMatches, targetDsOri, sparkSession)
    val hasDelAction = mergeMatches.matchList
      .exists(_.getActions.exists(_.isInstanceOf[DeleteAction]))
    val hasUpdateAction = mergeMatches.matchList
      .exists(_.getActions.exists(_.isInstanceOf[UpdateAction]))
    val (insertHistOfUpdate, insertHistOfDelete) = getInsertHistoryStatus(mergeMatches)
    // Update the update mapping with unfilled columns.From here on system assumes all mappings
    // are existed.
    mergeMatches = updateMappingIfNotExists(mergeMatches, targetDs)
    // Lets generate all conditions combinations as one column and add them as 'status'.
    val condition = generateStatusColumnWithAllCombinations(mergeMatches)

    // Add the getTupleId() udf to get the tuple id to generate delete delta.
    val frame = if (isMinMaxPruningEnabled && !didNotPrune) {
      targetDs
        .withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, expr("getTupleId()"))
        .withColumn("exist_on_target", lit(1))
        .where(s"block_paths('${finalCarbonFilesToScan.mkString(",")}')")
        .join(repartitionedSrcDs.withColumn("exist_on_src", lit(1)),
          mergeMatches.joinExpr,
          joinType)
        .withColumn(status_on_mergeds, condition)
    } else {
      targetDs
        .withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, expr("getTupleId()"))
        .withColumn("exist_on_target", lit(1))
        .join(repartitionedSrcDs.withColumn("exist_on_src", lit(1)),
          mergeMatches.joinExpr,
          joinType)
        .withColumn(status_on_mergeds, condition)
    }
    if (LOGGER.isDebugEnabled) {
      frame.explain()
    }
    val tableCols =
      targetCarbonTable.getCreateOrderColumn.asScala.map(_.getColName).
        filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    val header = tableCols.mkString(",")

    val frameWithoutStatusCol = frame.drop(status_on_mergeds)
    val projections: Seq[Seq[MergeProjection]] = mergeMatches.matchList.map { m =>
      m.getActions.map {
        case u: UpdateAction => MergeProjection(tableCols,
          frameWithoutStatusCol,
          relations.head,
          sparkSession,
          u)
        case i: InsertAction => MergeProjection(tableCols,
          frameWithoutStatusCol,
          relations.head,
          sparkSession,
          i)
        case d: DeleteAction => MergeProjection(tableCols,
          frameWithoutStatusCol,
          relations.head,
          sparkSession,
          d)
        case _ => null
      }.filter(_ != null)
    }

    val targetSchema = StructType(tableCols.map { f =>
      relations.head.carbonRelation.schema.find(_.name.equalsIgnoreCase(f)).get
    } ++ Seq(StructField(status_on_mergeds, IntegerType)))
    val (processedRDD, deltaPath) = processIUD(sparkSession, frame, targetCarbonTable, projections,
      targetSchema, stats)

    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val trxMgr = TranxManager(System.currentTimeMillis())

    val mutationAction = MutationActionFactory.getMutationAction(sparkSession,
      targetCarbonTable, hasDelAction, hasUpdateAction,
      insertHistOfUpdate, insertHistOfDelete)

    val loadDF = Dataset.ofRows(sparkSession,
      LogicalRDD(targetSchema.toAttributes,
        processedRDD)(sparkSession))

    loadDF.cache()
    val count = loadDF.count()
    val updateTableModel = if (FileFactory.isFileExist(deltaPath)) {
      val deltaRdd = AvroFileFormatFactory.readAvro(sparkSession, deltaPath)
      val tuple = mutationAction.handleAction(deltaRdd, executorErrors, trxMgr)
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(deltaPath))
      MergeUtil.updateSegmentStatusAfterUpdateOrDelete(targetCarbonTable,
        trxMgr.getLatestTrx, tuple)
      Some(UpdateTableModel(isUpdate = true, trxMgr.getLatestTrx,
        executorErrors, tuple._2, Option.empty))
    } else {
      None
    }

    val dataFrame = loadDF.select(tableCols.map(col): _*)
    MergeUtil.insertDataToTargetTable(sparkSession,
      targetCarbonTable,
      header,
      updateTableModel,
      dataFrame)

    if (hasDelAction && count == 0) {
      MergeUtil.updateStatusIfJustDeleteOperation(targetCarbonTable, trxMgr.getLatestTrx)
    }
    LOGGER.info(s"Total inserted rows: ${stats.insertedRows.sum}")
    LOGGER.info(s"Total updated rows: ${stats.updatedRows.sum}")
    LOGGER.info(s"Total deleted rows: ${stats.deletedRows.sum}")
    LOGGER.info(
      " Time taken to merge data  :: " + (System.currentTimeMillis() - st))

    // Load the history table if the insert history table action is added by user.
    HistoryTableLoadHelper.loadHistoryTable(sparkSession, relations.head, targetCarbonTable,
      trxMgr, mutationAction, mergeMatches)
    // Do IUD Compaction.
    HorizontalCompaction.tryHorizontalCompaction(
      sparkSession, targetCarbonTable)
    // clear the cached src
    repartitionedSrcDs.unpersist()
    Seq.empty
  }

  // Decide join type based on match conditions
  private def decideJoinType: String = {
    if (containsWhenNotMatchedOnly) {
      // if match condition contains WhenNotMatched only, then we do not need
      // left table key and matched key
      "right_outer"
    } else if (containsWhenMatchedOnly) {
      // if match condition contains WhenMatched only, then we need matched key only
      "inner"
    } else if (needKeyFromLeftTable) {
      // if we need to keep keys from left table, then use full outer join
      "full_outer"
    } else {
      // default join type
      "right"
    }
  }

  private def needKeyFromLeftTable: Boolean = {
    mergeMatches.matchList.exists(_.isInstanceOf[WhenNotMatchedAndExistsOnlyOnTarget])
  }

  private def containsWhenMatchedOnly: Boolean = {
    mergeMatches.matchList.forall(_.isInstanceOf[WhenMatched])
  }

  private def containsWhenNotMatchedOnly: Boolean = {
    mergeMatches.matchList.forall(_.isInstanceOf[WhenNotMatched])
  }

  /**
   * As per the status of the row either it inserts the data or update/delete the data.
   */
  private def processIUD(sparkSession: SparkSession,
      frame: DataFrame,
      carbonTable: CarbonTable,
      projections: Seq[Seq[MergeProjection]],
      targetSchema: StructType,
      stats: Stats): (RDD[InternalRow], String) = {
    val frameCols = frame.queryExecution.analyzed.output
    val tupleId = frameCols.zipWithIndex
      .find(_._1.name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)).get._2
    val insertedRows = stats.insertedRows
    val updatedRows = stats.updatedRows
    val deletedRows = stats.deletedRows
    val job = CarbonSparkUtil.createHadoopJob()
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    val uuid = UUID.randomUUID.toString
    job.setJobID(new JobID(uuid, 0))
    val path = carbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR + "avro"
    FileOutputFormat.setOutputPath(job, new Path(path))
    val schema =
      org.apache.spark.sql.types.StructType(Seq(
        StructField(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, StringType),
        StructField(status_on_mergeds, IntegerType)))
    val factory = AvroFileFormatFactory.getAvroWriter(sparkSession, job, schema)
    val config = SparkSQLUtil.broadCastHadoopConf(sparkSession.sparkContext, job.getConfiguration)
    val expr = frame.queryExecution.sparkPlan.asInstanceOf[ProjectExec].projectList.last
    val frameWithoutStatusCol = frame.drop(status_on_mergeds)
    val colSchemaWithoutStatusCol = frameWithoutStatusCol.queryExecution.logical.output
    (frameWithoutStatusCol.queryExecution.toRdd.mapPartitionsWithIndex { case (index, iter) =>
      val confB = config.value.value
      val task = new TaskID(new JobID(uuid, 0), TaskType.MAP, index)
      val attemptID = new TaskAttemptID(task, index)
      val context = new TaskAttemptContextImpl(confB, attemptID)
      val writer = factory.newInstance(path + CarbonCommonConstants.FILE_SEPARATOR + task.toString,
        schema, context)
      val projLen = projections.length
        new Iterator[InternalRow] {
          val queue = new util.LinkedList[InternalRow]()

          override def hasNext: Boolean = {
            if (!queue.isEmpty || iter.hasNext) {
              true
            } else {
              writer.close()
              false
            }
          }

          override def next(): InternalRow = {
            if (!queue.isEmpty) {
              return queue.poll()
            }
            val row = iter.next()
            val is = CarbonToSparkAdapter.evaluateWithPredicate(expr,
              colSchemaWithoutStatusCol, row)
            var isUpdate = false
            var isDelete = false
            var insertedCount = 0
            if (is != null) {
              val isInt = is.asInstanceOf[Int]
              var i = 0
              while (i < projLen) {
                if ((isInt & (1 << i)) == (1 << i)) {
                  projections(i).foreach { p =>
                    if (!p.isDelete) {
                      if (p.isUpdate) {
                        isUpdate = p.isUpdate
                      }
                      queue.add(p.getInternalRowFromIndex(row, is.asInstanceOf[Int]))
                      insertedCount += 1
                    } else {
                      isDelete = true
                    }
                  }
                }
                i = i + 1
              }
            }
            val newArray = new Array[Any](2)
            newArray(0) = row.getUTF8String(tupleId)
            if (isUpdate && isDelete) {
              newArray(1) = 102
              writer.write(new GenericInternalRow(newArray))
              updatedRows.add(1)
              deletedRows.add(1)
              insertedCount -= 1
            } else if (isUpdate) {
              updatedRows.add(1)
              newArray(1) = 101
              insertedCount -= 1
              writer.write(new GenericInternalRow(newArray))
            } else if (isDelete) {
              newArray(1) = 100
              deletedRows.add(1)
              writer.write(new GenericInternalRow(newArray))
            }
            insertedRows.add(insertedCount)
            if (!queue.isEmpty) {
              queue.poll()
            } else {
              val values = new Array[Any](targetSchema.length)
              new GenericInternalRow(values)
            }
          }
        }
    }.filter { row => !row.isNullAt(targetSchema.length - 1)}, path)
  }

  private def createLongAccumulator(name: String) = {
    val acc = new LongAccumulator
    acc.setValue(0)
    acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), countFailedValues
      = false)
    AccumulatorContext.register(acc)
    acc
  }

  /**
   * It generates conditions for all possible scenarios and add a integer number for each match.
   * There could be scenarios that one row can match two conditions so it should apply the actions
   * of both the matches to the row.
   *  For example :
   *    whenmathed(a=c1)
   *    update()
   *    whennotmatched(b=d1)
   *    insert()
   *    whennotmatched(b=d2)
   *    insert()
   *
   *  The above merge statement will be converted to
   *    (case when a=c1 and b=d1 and b=d2 then 7
   *         when a=c1 and b=d1 then 6
   *         when a=c1 and b=d2 then 5
   *         when a=c1 then 4
   *         when b=d1 and b=d2 then 3
   *         when b=d1 then 2
   *         when b=d2 the 1) as status
   *
   *   So it would not be recommended use so many merge conditions as it increase the case when
   *   statements exponentially.
   *
   * @param mergeMatches
   * @return
   */
  def generateStatusColumnWithAllCombinations(mergeMatches: MergeDataSetMatches): Column = {
    var exprList = new ArrayBuffer[(Column, Int)]()
    val matchList = mergeMatches.matchList
    val len = matchList.length
    val N = Math.pow(2d, len.toDouble).toInt
    var i = 1
    while (i < N) {
      var status = 0
      var column: Column = null
      val code = Integer.toBinaryString(N | i).substring(1)
      var j = 0
      while (j < len) {
        if (code.charAt(j) == '1') {
          val mergeMatch = matchList(j)
          if (column == null) {
            if (mergeMatch.getExp.isDefined) {
              column = mergeMatch.getExp.get
            }
          } else {
            if (mergeMatch.getExp.isDefined) {
              column = column.and(mergeMatch.getExp.get)
            }
          }
          mergeMatch match {
            case wm: WhenMatched =>
              val existsOnBoth = col("exist_on_target").isNotNull.and(
                col("exist_on_src").isNotNull)
              column = if (column == null) {
                existsOnBoth
              } else {
                column.and(existsOnBoth)
              }
            case wnm: WhenNotMatched =>
              val existsOnSrc = col("exist_on_target").isNull.and(
                col("exist_on_src").isNotNull)
              column = if (column == null) {
                existsOnSrc
              } else {
                column.and(existsOnSrc)
              }
            case wnm: WhenNotMatchedAndExistsOnlyOnTarget =>
              val existsOnSrc = col("exist_on_target").isNotNull.and(
                col("exist_on_src").isNull)
              column = if (column == null) {
                existsOnSrc
              } else {
                column.and(existsOnSrc)
              }
            case _ =>
          }
          status = status | 1 << j
        }
        j += 1
      }
      if (column == null) {
        column = lit(true) === lit(true)
      }
      exprList += ((column, status))
      i += 1
    }
    exprList = exprList.reverse
    var condition: Column = null
    exprList.foreach { case (col, status) =>
      if (condition == null) {
        condition = when(col, lit(status))
      } else {
        condition = condition.when(col, lit(status))
      }
    }
    condition.otherwise(lit(null))
  }

  private def getSelectExpressionsOnExistingDF(existingDs: Dataset[Row],
      mergeMatches: MergeDataSetMatches, sparkSession: SparkSession): Seq[Column] = {
    var projects = Seq.empty[Attribute]
    val existAttrs = existingDs.queryExecution.analyzed.output
    projects ++= selectAttributes(mergeMatches.joinExpr.expr, existingDs, sparkSession)
    mergeMatches.matchList.foreach { m =>
      if (m.getExp.isDefined) {
        projects ++= selectAttributes(m.getExp.get.expr, existingDs, sparkSession)
      }
      m.getActions.foreach {
        case u: UpdateAction =>
          projects ++= existAttrs.filterNot { f =>
            u.updateMap.exists(_._1.toString().equalsIgnoreCase(f.name))
          }
        case i: InsertAction =>
          if (!existAttrs.forall(f => i.insertMap
            .exists(_._1.toString().equalsIgnoreCase(f.name)))) {
            throw new CarbonMergeDataSetException(
              "Not all source columns are mapped for insert action " + i.insertMap)
          }
          i.insertMap.foreach { case (k, v) =>
            projects ++= selectAttributes(v.expr, existingDs, sparkSession)
          }
        case _ =>
      }
    }
    projects.map(_.name.toLowerCase).distinct.map { p =>
      existingDs.col(p)
    }
  }

  private def updateMappingIfNotExists(mergeMatches: MergeDataSetMatches,
      existingDs: Dataset[Row]): MergeDataSetMatches = {
    val existAttrs = existingDs.queryExecution.analyzed.output
    val updateCommand = mergeMatches.matchList.map { m =>
      val updateAction = m.getActions.map {
        case u: UpdateAction =>
          if (u.updateMap.isEmpty) {
            throw new CarbonMergeDataSetException(
              "At least one column supposed to be updated for update action")
          }
          val attributes = existAttrs.filterNot { f =>
            u.updateMap.exists(_._1.toString().equalsIgnoreCase(f.name))
          }
          val newMap = attributes.map(a => (existingDs.col(a.name), existingDs.col(a.name))).toMap
          u.copy(u.updateMap ++ newMap)
        case other => other
      }
      m.updateActions(updateAction)
    }
    mergeMatches.copy(matchList =
      updateCommand.filterNot(_.getActions.exists(_.isInstanceOf[DeleteAction]))
      ++ updateCommand.filter(_.getActions.exists(_.isInstanceOf[DeleteAction])))
  }

  private def selectAttributes(expression: Expression, existingDs: Dataset[Row],
      sparkSession: SparkSession, throwError: Boolean = false) = {
    expression.collect {
      case a: Attribute =>
        val resolved = existingDs.queryExecution
          .analyzed.resolveQuoted(a.name, sparkSession.sessionState.analyzer.resolver)
        if (resolved.isDefined) {
          resolved.get.toAttribute
        } else if (throwError) {
          throw new CarbonMergeDataSetException(
            expression + " cannot be resolved with dataset " + existingDs)
        } else {
          null
        }
    }.filter(_ != null)
  }

  private def getInsertHistoryStatus(mergeMatches: MergeDataSetMatches): (Boolean, Boolean) = {
    val insertHistOfUpdate = mergeMatches.matchList.exists(p =>
      p.getActions.exists(_.isInstanceOf[InsertInHistoryTableAction])
      && p.getActions.exists(_.isInstanceOf[UpdateAction]))
    val insertHistOfDelete = mergeMatches.matchList.exists(p =>
      p.getActions.exists(_.isInstanceOf[InsertInHistoryTableAction])
      && p.getActions.exists(_.isInstanceOf[DeleteAction]))
    (insertHistOfUpdate, insertHistOfDelete)
  }

  private def validateMergeActions(mergeMatches: MergeDataSetMatches,
      existingDs: Dataset[Row], sparkSession: SparkSession): Unit = {
    val existAttrs = existingDs.queryExecution.analyzed.output
    if (mergeMatches.matchList.exists(m => m.getActions.exists(_.isInstanceOf[DeleteAction])
                                           && m.getActions.exists(_.isInstanceOf[UpdateAction]))) {
      throw new AnalysisException(
        "Delete and update action should not be under same merge condition")
    }
    if (mergeMatches.matchList.count(m => m.getActions.exists(_.isInstanceOf[DeleteAction])) > 1) {
      throw new AnalysisException("Delete action should not be more than once across merge")
    }
    mergeMatches.matchList.foreach { f =>
      if (f.getActions.exists(_.isInstanceOf[InsertInHistoryTableAction])) {
        if (!(f.getActions.exists(_.isInstanceOf[UpdateAction]) ||
              f.getActions.exists(_.isInstanceOf[DeleteAction]))) {
          throw new AnalysisException("For inserting to history table, " +
                              "it should be along with either update or delete action")
        }
        val value = f.getActions.find(_.isInstanceOf[InsertInHistoryTableAction]).get.
          asInstanceOf[InsertInHistoryTableAction]
        if (!existAttrs.forall(f => value.insertMap
          .exists(_._1.toString().equalsIgnoreCase(f.name)))) {
          throw new AnalysisException(
            "Not all source columns are mapped for insert action " + value.insertMap)
        }
        value.insertMap.foreach { case (k, v) =>
          selectAttributes(v.expr, existingDs, sparkSession, throwError = true)
        }
      }
    }
  }

  override protected def opName: String = "MERGE DATASET"
}
