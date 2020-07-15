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
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, CarbonUtils, Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.SparkCarbonFileFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.command.{DataCommand, ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.execution.command.mutation.HorizontalCompaction
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, LongAccumulator}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.FailureCauses

/**
 * This command will merge the data of source dataset to target dataset backed by carbon table.
 * @param targetDsOri Target dataset to merge the data. This dataset should be backed by carbontable
 * @param srcDS  Source dataset, it can be any data.
 * @param mergeMatches It contains the join condition and list match conditions to apply.
 */
case class CarbonMergeDataSetCommand(
    targetDsOri: Dataset[Row],
    srcDS: Dataset[Row],
    var mergeMatches: MergeDataSetMatches)
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
    val rltn = CarbonUtils.collectCarbonRelation(targetDsOri.logicalPlan)
    // Target dataset must be backed by carbondata table.
    if (rltn.length != 1) {
      throw new UnsupportedOperationException(
        "Carbon table supposed to be present in merge dataset")
    }
    // validate the merge matches and actions.
    validateMergeActions(mergeMatches, targetDsOri, sparkSession)
    val carbonTable = rltn.head.carbonRelation.carbonTable
    val hasDelAction = mergeMatches.matchList
      .exists(_.getActions.exists(_.isInstanceOf[DeleteAction]))
    val hasUpdateAction = mergeMatches.matchList
      .exists(_.getActions.exists(_.isInstanceOf[UpdateAction]))
    val (insertHistOfUpdate, insertHistOfDelete) = getInsertHistoryStatus(mergeMatches)
    // Get all the required columns of targetDS by going through all match conditions and actions.
    val columns = getSelectExpressionsOnExistingDF(targetDsOri, mergeMatches, sparkSession)
    // select only the required columns, it can avoid lot of and shuffling.
    val targetDs = targetDsOri.select(columns: _*)
    // Update the update mapping with unfilled columns.From here on system assumes all mappings
    // are existed.
    mergeMatches = updateMappingIfNotExists(mergeMatches, targetDs)
    // Lets generate all conditions combinations as one column and add them as 'status'.
    val condition = generateStatusColumnWithAllCombinations(mergeMatches)

    // decide join type based on match conditions
    val joinType = decideJoinType

    // Add the tupleid udf to get the tupleid to generate delete delta.
    val frame =
      targetDs
        .withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, expr("getTupleId()"))
        .withColumn("exist_on_target", lit(1))
        .join(srcDS.withColumn("exist_on_src", lit(1)), mergeMatches.joinExpr, joinType)
        .withColumn(status_on_mergeds, condition)
    if (LOGGER.isDebugEnabled) {
      frame.explain()
    }
    val tableCols =
      carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala.map(_.getColumnName).
        filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    val header = tableCols.mkString(",")

    val projections: Seq[Seq[MergeProjection]] = mergeMatches.matchList.map { m =>
      m.getActions.map {
        case u: UpdateAction => MergeProjection(tableCols,
          status_on_mergeds,
          frame,
          rltn.head,
          sparkSession,
          u)
        case i: InsertAction => MergeProjection(tableCols,
          status_on_mergeds,
          frame,
          rltn.head,
          sparkSession,
          i)
        case d: DeleteAction => MergeProjection(tableCols,
          status_on_mergeds,
          frame,
          rltn.head,
          sparkSession,
          d)
        case _ => null
      }.filter(_ != null)
    }

    val st = System.currentTimeMillis()
    // Create accumulators to log the stats
    val stats = Stats(createLongAccumalator("insertedRows"),
      createLongAccumalator("updatedRows"),
      createLongAccumalator("deletedRows"))
    val targetSchema = StructType(tableCols.map { f =>
      rltn.head.carbonRelation.schema.find(_.name.equalsIgnoreCase(f)).get
    } ++ Seq(StructField(status_on_mergeds, IntegerType)))
    val (processedRDD, deltaPath) = processIUD(sparkSession, frame, carbonTable, projections,
      targetSchema, stats)

    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val trxMgr = TranxManager(System.currentTimeMillis())

    val mutationAction = MutationActionFactory.getMutationAction(sparkSession,
      carbonTable, hasDelAction, hasUpdateAction,
      insertHistOfUpdate, insertHistOfDelete)

    val loadDF = Dataset.ofRows(sparkSession,
      LogicalRDD(targetSchema.toAttributes,
        processedRDD)(sparkSession))

    loadDF.cache()
    val count = loadDF.count()
    val updateTableModel = if (FileFactory.isFileExist(deltaPath)) {
      val deltaRdd = sparkSession.read.format("carbon").load(deltaPath).rdd
      val tuple = mutationAction.handleAction(deltaRdd, executorErrors, trxMgr)
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(deltaPath))
      if (!CarbonUpdateUtil.updateSegmentStatus(tuple._1.asScala.asJava,
        carbonTable,
        trxMgr.getLatestTrx.toString, false)) {
        LOGGER.error("writing of update status file failed")
        throw new CarbonMergeDataSetException("writing of update status file failed")
      }
      if (carbonTable.isHivePartitionTable) {
        // If load count is 0 and if merge action contains delete operation, update
        // tableUpdateStatus file name in loadMeta entry
        if (count == 0 && hasDelAction && !tuple._1.isEmpty) {
          val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
            .getTableStatusFilePath(carbonTable.getTablePath))
          CarbonUpdateUtil.updateTableMetadataStatus(loadMetaDataDetails.map(loadMetadataDetail =>
            new Segment(loadMetadataDetail.getMergedLoadName,
              loadMetadataDetail.getSegmentFile)).toSet.asJava,
            carbonTable,
            trxMgr.getLatestTrx.toString,
            true,
            tuple._2.asJava)
        }
      }
      Some(UpdateTableModel(true, trxMgr.getLatestTrx,
        executorErrors, tuple._2, true))
    } else {
      None
    }

    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonTable.getDatabaseName),
      tableName = carbonTable.getTableName,
      options = Map("fileheader" -> header, "sort_scope" -> "nosort"),
      isOverwriteTable = false,
      dataFrame = loadDF.select(tableCols.map(col): _*),
      updateModel = updateTableModel,
      tableInfoOp = Some(carbonTable.getTableInfo)).process(sparkSession)

    LOGGER.info(s"Total inserted rows: ${stats.insertedRows.sum}")
    LOGGER.info(s"Total updated rows: ${stats.updatedRows.sum}")
    LOGGER.info(s"Total deleted rows: ${stats.deletedRows.sum}")
    LOGGER.info(
      " Time taken to merge data  :: " + (System.currentTimeMillis() - st))

    // Load the history table if the inserthistorytable action is added by user.
    HistoryTableLoadHelper.loadHistoryTable(sparkSession, rltn.head, carbonTable,
      trxMgr, mutationAction, mergeMatches)
    // Do IUD Compaction.
    HorizontalCompaction.tryHorizontalCompaction(
      sparkSession, carbonTable, isUpdateOperation = false)
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
      stats: Stats) = {
    val conf = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    val frameCols = frame.queryExecution.analyzed.output
    val status = frameCols.length - 1
    val tupleId = frameCols.zipWithIndex
      .find(_._1.name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)).get._2
    val insertedRows = stats.insertedRows
    val updatedRows = stats.updatedRows
    val deletedRows = stats.deletedRows
    val job = Job.getInstance(conf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    val uuid = UUID.randomUUID.toString
    job.setJobID(new JobID(uuid, 0))
    val path = carbonTable.getTablePath + "/" + job.getJobID
    FileOutputFormat.setOutputPath(job, new Path(path))
    val schema =
      org.apache.spark.sql.types.StructType(Seq(
        StructField(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, StringType),
        StructField(status_on_mergeds, IntegerType)))
    val factory =
      new SparkCarbonFileFormat().prepareWrite(sparkSession, job,
        Map(), schema)
    val config = SparkSQLUtil.broadCastHadoopConf(sparkSession.sparkContext, job.getConfiguration)
    (frame.rdd.coalesce(DistributionUtil.getConfiguredExecutors(sparkSession.sparkContext)).
      mapPartitionsWithIndex { case (index, iter) =>
        var directlyWriteDataToHdfs = CarbonProperties.getInstance()
          .getProperty(CarbonLoadOptionConstants
            .ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH, CarbonLoadOptionConstants
            .ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH_DEFAULT)
        CarbonProperties.getInstance().addProperty(CarbonLoadOptionConstants
          .ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH, "true")
        val confB = config.value.value
        val task = new TaskID(new JobID(uuid, 0), TaskType.MAP, index)
        val attemptID = new TaskAttemptID(task, index)
        val context = new TaskAttemptContextImpl(confB, attemptID)
        val writer = factory.newInstance(path, schema, context)
        val projLen = projections.length
        new Iterator[InternalRow] {
          val queue = new util.LinkedList[InternalRow]()
          override def hasNext: Boolean = if (!queue.isEmpty || iter.hasNext) true else {
            writer.close()
            // revert load direct write to store path after insert
            CarbonProperties.getInstance().addProperty(CarbonLoadOptionConstants
              .ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH, directlyWriteDataToHdfs)
            false
          }

          override def next(): InternalRow = {
            if (!queue.isEmpty) return queue.poll()
            val row = iter.next()
            val rowWithSchema = row.asInstanceOf[GenericRowWithSchema]
            val is = row.get(status)
            var isUpdate = false
            var isDelete = false
            var insertedCount = 0
            if (is != null) {
              val isInt = is.asInstanceOf[Int]
              var i = 0
              while (i < projLen) {
                if ((isInt & (1 << i)) == (1 << i)) projections(i).foreach { p =>
                  if (!p.isDelete) {
                    if (p.isUpdate) isUpdate = p.isUpdate
                    queue.add(p(rowWithSchema))
                    insertedCount += 1
                  } else isDelete = true
                }
                i = i + 1
              }
            }
            val newArray = new Array[Any](2)
            newArray(0) = UTF8String.fromString(row.getString(tupleId))
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
            if (!queue.isEmpty) queue.poll() else {
              val values = new Array[Any](targetSchema.length)
              new GenericInternalRow(values)
            }
          }
        }
      }.filter { row =>
      val status = row.get(targetSchema.length-1, IntegerType)
      status != null
    }, path)
  }

  private def createLongAccumalator(name: String) = {
    val acc = new LongAccumulator
    acc.setValue(0)
    acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), false)
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

  private def collectCarbonRelation(plan: LogicalPlan): Seq[CarbonDatasourceHadoopRelation] = {
    plan collect {
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    }
  }

  private def getInsertHistoryStatus(mergeMatches: MergeDataSetMatches) = {
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
          selectAttributes(v.expr, existingDs, sparkSession, true)
        }
      }
    }
  }

  override protected def opName: String = "MERGE DATASET"
}
