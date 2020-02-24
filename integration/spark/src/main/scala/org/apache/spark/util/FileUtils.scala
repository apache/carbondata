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

package org.apache.spark.util

import java.io.{File, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.DatabaseLocationProvider
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{CreateDatabasePostExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.exception.DataLoadingException

object FileUtils {
  /**
   * append all csv file path to a String, file path separated by comma
   */
  private def getPathsFromCarbonFile(
      carbonFile: CarbonFile,
      stringBuild: StringBuilder,
      hadoopConf: Configuration): Unit = {
    if (carbonFile.isDirectory) {
      val files = carbonFile.listFiles()
      for (j <- 0 until files.size) {
        getPathsFromCarbonFile(files(j), stringBuild, hadoopConf)
      }
    } else {
      val path = carbonFile.getAbsolutePath
      val fileName = carbonFile.getName
      if (carbonFile.getSize == 0) {
        LogServiceFactory.getLogService(this.getClass.getCanonicalName)
            .warn(s"skip empty input file: ${CarbonUtil.removeAKSK(path)}")
      } else if (fileName.startsWith(CarbonCommonConstants.UNDERSCORE) ||
                 fileName.startsWith(CarbonCommonConstants.POINT)) {
        LogServiceFactory.getLogService(this.getClass.getCanonicalName)
            .warn(s"skip invisible input file: ${CarbonUtil.removeAKSK(path)}")
      } else {
        stringBuild.append(path.replace('\\', '/')).append(CarbonCommonConstants.COMMA)
      }
    }
  }

  /**
   * append all file path to a String, inputPath path separated by comma
   *
   */
  def getPaths(inputPath: String): String = {
    getPaths(inputPath, FileFactory.getConfiguration)
  }

  def getPaths(inputPath: String, hadoopConf: Configuration): String = {
    if (inputPath == null || inputPath.isEmpty) {
      throw new DataLoadingException("Input file path cannot be empty.")
    } else {
      val stringBuild = new StringBuilder()
      val filePaths = inputPath.split(",").map(_.trim)
      for (i <- 0 until filePaths.size) {
        val filePath = CarbonUtil.checkAndAppendHDFSUrl(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePath, hadoopConf)
        if (!carbonFile.exists()) {
          throw new DataLoadingException(
            s"The input file does not exist: ${CarbonUtil.removeAKSK(filePaths(i))}" )
        }
        getPathsFromCarbonFile(carbonFile, stringBuild, hadoopConf)
      }
      if (stringBuild.nonEmpty) {
        stringBuild.substring(0, stringBuild.size - 1)
      } else {
        throw new DataLoadingException("Please check your input path and make sure " +
                                       "that files end with '.csv' and content is not empty.")
      }
    }
  }

  def getSpaceOccupied(inputPath: String, hadoopConfiguration: Configuration): Long = {
    var size : Long = 0
    if (inputPath == null || inputPath.isEmpty) {
      size
    } else {
      val filePaths = inputPath.split(",")
      for (i <- 0 until filePaths.size) {
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), hadoopConfiguration)
        size = size + carbonFile.getSize
      }
      size
    }
  }

  def createDatabaseDirectory(dbName: String, storePath: String, sparkContext: SparkContext) {
    val databasePath: String =
      storePath + File.separator + DatabaseLocationProvider.get().provide(dbName.toLowerCase)
    FileFactory.mkdirs(databasePath)
    val operationContext = new OperationContext
    val createDatabasePostExecutionEvent = new CreateDatabasePostExecutionEvent(dbName,
      databasePath, sparkContext)
    OperationListenerBus.getInstance.fireEvent(createDatabasePostExecutionEvent, operationContext)
  }

}
