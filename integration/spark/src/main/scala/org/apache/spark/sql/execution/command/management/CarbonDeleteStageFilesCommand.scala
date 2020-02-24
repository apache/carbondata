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

package org.apache.spark.sql.execution.command.management

import java.io.InputStreamReader
import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.statusmanager.StageInput
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Delete carbon data files of table stages.
 *
 * @param databaseNameOp database name
 * @param tableName      table name
 */
case class CarbonDeleteStageFilesCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String]
) extends DataCommand {

  @transient val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processData(spark: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, spark)
    val table = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(spark)
    val configuration = spark.sessionState.newHadoopConf()
    setAuditTable(table)
    if (!table.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (table.isChildTableForMV) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV table")
    }
    val tablePath = table.getTablePath
    val startTime = System.currentTimeMillis()
    val stageDataFileActiveTime = try {
      Integer.valueOf(options.getOrElse("retain_hour", "0")) * 3600000
    } catch {
      case _: NumberFormatException =>
        throw new MalformedCarbonCommandException(
          "Option [retain_hour] is not a number.")
    }
    if (stageDataFileActiveTime < 0) {
      throw new MalformedCarbonCommandException(
        "Option [retain_hour] is negative.")
    }
    val stageDataFilesReferenced =
      listStageDataFilesReferenced(listStageMetadataFiles(tablePath, configuration), configuration)
    val stageDataFiles = listStageDataFiles(tablePath, configuration)
    stageDataFiles.collect {
      case stageDataFile: CarbonFile =>
        // Which file will be deleted:
        // 1. Not referenced by any stage file;
        // 2. Has passed retain time.
        if (!stageDataFilesReferenced.contains(stageDataFile.getCanonicalPath) &&
            (startTime - stageDataFile.getLastModifiedTime) >= stageDataFileActiveTime) {
          stageDataFile.delete()
        }
    }
    Seq.empty
  }

  private def listStageMetadataFiles(
      tablePath: String,
      configuration: Configuration
  ): Seq[CarbonFile] = {
    val stagePath = CarbonTablePath.getStageDir(tablePath)
    val stageDirectory = FileFactory.getCarbonFile(stagePath, configuration)
    if (stageDirectory.exists()) {
      stageDirectory.listFiles().filter { file =>
        !file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }
    } else {
      Seq.empty
    }
  }

  private def listStageDataFiles(
      tablePath: String,
      configuration: Configuration
  ): Seq[CarbonFile] = {
    val stageDataFileLocation = FileFactory.getCarbonFile(
      CarbonTablePath.getStageDataDir(tablePath),
      configuration
    )
    if (!stageDataFileLocation.exists()) {
      LOGGER.warn(
        "Stage data file location is not exists. " + CarbonTablePath.getStageDataDir(tablePath)
      )
      Seq.empty
    } else {
      stageDataFileLocation.listFiles(true).asScala
    }
  }

  /**
   * Collect data file path list which referenced by stage (which is not loaded into the table).
   */
  private def listStageDataFilesReferenced(
      stageFiles: Seq[CarbonFile],
      configuration: Configuration
  ): Set[String] = {
    if (stageFiles.isEmpty) {
      return Set.empty
    }
    // Collect stage data files.
    val stageDataFilesReferenced = Collections.synchronizedSet(new util.HashSet[String]())
    val startTime = System.currentTimeMillis()
    stageFiles.foreach { stageFile =>
      val stream = FileFactory.getDataInputStream(stageFile.getAbsolutePath, configuration)
      try {
        val stageInput =
          new Gson().fromJson(new InputStreamReader(stream), classOf[StageInput])
        val stageDataBase = stageInput.getBase + CarbonCommonConstants.FILE_SEPARATOR
        if (stageInput.getFiles != null) {
          // For non-partition table.
          stageInput.getFiles.asScala.foreach(
            stageDataFile =>
              stageDataFilesReferenced.add(
                FileFactory.getCarbonFile(
                  stageDataBase + stageDataFile._1,
                  configuration
                ).getCanonicalPath
              )
          )
        }
        if (stageInput.getLocations != null) {
          // For partition table.
          stageInput.getLocations.asScala.foreach(
            stageDataLocation =>
              stageDataLocation.getFiles.asScala.foreach(
                stageDataFile =>
                  stageDataFilesReferenced.add(
                    FileFactory.getCarbonFile(
                      stageDataBase + stageDataFile._1,
                      configuration
                    ).getCanonicalPath
                  )
              )
          )
        }
      } finally {
        stream.close()
      }
    }
    LOGGER.info(s"Read stage files taken ${ System.currentTimeMillis() - startTime }ms.")
    stageDataFilesReferenced.asScala.toSet
  }

  override protected def opName: String = "DELETE STAGE"
}
