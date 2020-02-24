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

import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, CarbonUtils, Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{DataCommand, ExecutionErrors}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, LongAccumulator}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

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

    // Add the tupleid udf to get the tupleid to generate delete delta.
    val frame = targetDs.withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
      expr("getTupleId()")).withColumn("exist_on_target", lit(1)).join(
      srcDS.withColumn("exist_on_src", lit(1)),
      // Do the full outer join to get the data from both sides without missing anything.
      // TODO As per the match conditions choose the join, sometimes it might be possible to use
      // left_outer join.
      mergeMatches.joinExpr, "full_outer").withColumn("status", condition)
    if (LOGGER.isDebugEnabled) {
      frame.explain()
    }
    val tableCols =
      carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala.map(_.getColumnName).
        filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    val builder = new CarbonLoadModelBuilder(carbonTable)
    val options = Seq(("fileheader", tableCols.mkString(","))).toMap
    val model = builder.build(options.asJava, CarbonUpdateUtil.readCurrentTime, "1")
    model.setLoadWithoutConverterStep(true)
    val newLoadMetaEntry = new LoadMetadataDetails
    CarbonLoaderUtil.populateNewLoadMetaEntry(newLoadMetaEntry,
      SegmentStatus.INSERT_IN_PROGRESS,
      model.getFactTimeStamp,
      false)
    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false)

    model.setCsvHeader(tableCols.mkString(","))

    val projections: Seq[Seq[MergeProjection]] = mergeMatches.matchList.map { m =>
      m.getActions.map {
        case u: UpdateAction => MergeProjection(tableCols, frame, rltn.head, sparkSession, u)
        case i: InsertAction => MergeProjection(tableCols, frame, rltn.head, sparkSession, i)
        case d: DeleteAction => MergeProjection(tableCols, frame, rltn.head, sparkSession, d)
        case _ => null
      }.filter(_ != null)
    }

    val st = System.currentTimeMillis()
    // Create accumulators to log the stats
    val stats = Stats(createLongAccumalator("insertedRows"),
      createLongAccumalator("updatedRows"),
      createLongAccumalator("deletedRows"))
    val processedRDD = processIUD(sparkSession, frame, carbonTable, model, projections, stats)

    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val trxMgr = TranxManager(model.getFactTimeStamp)

    val mutationAction = MutationActionFactory.getMutationAction(sparkSession,
      carbonTable, hasDelAction, hasUpdateAction,
      insertHistOfUpdate, insertHistOfDelete)

    val tuple = mutationAction.handleAction(processedRDD, executorErrors, trxMgr)

    // In case user only has insert action.
    if (!(hasDelAction || hasUpdateAction)) {
      processedRDD.count()
    }
    LOGGER.info(s"Total inserted rows: ${stats.insertedRows.sum}")
    LOGGER.info(s"Total updated rows: ${stats.updatedRows.sum}")
    LOGGER.info(s"Total deleted rows: ${stats.deletedRows.sum}")
    LOGGER.info(
      " Time taken to merge data  : " + tuple + " :: " + (System.currentTimeMillis() - st))

    val segment = new Segment(model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      CarbonTablePath.getSegmentPath(carbonTable.getTablePath,
        model.getSegmentId), Map.empty[String, String].asJava)
    val writeSegment =
      SegmentFileStore.writeSegmentFile(carbonTable, segment)

    if (writeSegment) {
      SegmentFileStore.updateTableStatusFile(
        carbonTable,
        model.getSegmentId,
        segment.getSegmentFileName,
        carbonTable.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName),
        SegmentStatus.SUCCESS)
    } else {
      CarbonLoaderUtil.updateTableStatusForFailure(model)
    }

    if (hasDelAction || hasUpdateAction) {
      if (CarbonUpdateUtil.updateSegmentStatus(tuple._1, carbonTable,
        trxMgr.getLatestTrx.toString, false) &&
          CarbonUpdateUtil
            .updateTableMetadataStatus(
              model.getLoadMetadataDetails.asScala.map(l =>
                new Segment(l.getMergedLoadName,
                  l.getSegmentFile)).toSet.asJava,
              carbonTable,
              trxMgr.getLatestTrx.toString,
              true,
              tuple._2.asJava)) {
        LOGGER.info(s"Merge data operation is successful for " +
                    s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
      } else {
        throw new CarbonMergeDataSetException("Saving update status or table status failed")
      }
    }
    // Load the history table if the inserthistorytable action is added by user.
    HistoryTableLoadHelper.loadHistoryTable(sparkSession, rltn.head, carbonTable,
      trxMgr, mutationAction, mergeMatches)
    Seq.empty
  }

  /**
   * As per the status of the row either it inserts the data or update/delete the data.
   */
  private def processIUD(sparkSession: SparkSession,
      frame: DataFrame,
      carbonTable: CarbonTable,
      model: CarbonLoadModel,
      projections: Seq[Seq[MergeProjection]],
      stats: Stats) = {
    val conf = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    val config = SparkSQLUtil.broadCastHadoopConf(sparkSession.sparkContext, conf)
    val frameCols = frame.queryExecution.analyzed.output
    val status = frameCols.length - 1
    val tupleId = frameCols.zipWithIndex
      .find(_._1.name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)).get._2
    val insertedRows = stats.insertedRows
    val updatedRows = stats.updatedRows
    val deletedRows = stats.deletedRows
    frame.rdd.coalesce(DistributionUtil.getConfiguredExecutors(sparkSession.sparkContext)).
      mapPartitionsWithIndex { case (index, iter) =>
        val confB = config.value.value
        CarbonTableOutputFormat.setCarbonTable(confB, carbonTable)
        model.setTaskNo(index.toString)
        CarbonTableOutputFormat.setLoadModel(confB, model)
        val jobId = new JobID(UUID.randomUUID.toString, 0)
        val task = new TaskID(jobId, TaskType.MAP, index)
        val attemptID = new TaskAttemptID(task, index)
        val context = new TaskAttemptContextImpl(confB, attemptID)
        val writer = new CarbonTableOutputFormat().getRecordWriter(context)
        val writable = new ObjectArrayWritable
        val projLen = projections.length
        val schema =
          org.apache.spark.sql.types.StructType(Seq(
            StructField(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID, StringType),
            StructField("status", IntegerType)))
        new Iterator[Row] {
          override def hasNext: Boolean = {
            if (iter.hasNext) {
              true
            } else {
              writer.close(context)
              false
            }
          }

          override def next(): Row = {
            val row = iter.next()
            val rowWithSchema = row.asInstanceOf[GenericRowWithSchema]
            val is = row.get(status)
            var isUpdate = false
            var isDelete = false
            var insertedCount = 0
            if (is != null) {
              val isInt = is.asInstanceOf[Int]
              var i = 0;
              while (i < projLen) {
                if ((isInt & (1 << i)) == (1 << i)) {
                  projections(i).foreach { p =>
                    if (!p.isDelete) {
                      if (p.isUpdate) {
                        isUpdate = p.isUpdate
                      }
                      writable.set(p(rowWithSchema))
                      writer.write(NullWritable.get(), writable)
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
            newArray(0) = row.getString(tupleId)
            if (isUpdate && isDelete) {
              newArray(1) = 102
              updatedRows.add(1)
              deletedRows.add(1)
              insertedCount -= 1
            } else if (isUpdate) {
              updatedRows.add(1)
              newArray(1) = 101
              insertedCount -= 1
            } else if (isDelete) {
              newArray(1) = 100
              deletedRows.add(1)
            } else {
              newArray(1) = is
            }
            insertedRows.add(insertedCount)
            new GenericRowWithSchema(newArray, schema)
          }
        }
      }.cache()
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
