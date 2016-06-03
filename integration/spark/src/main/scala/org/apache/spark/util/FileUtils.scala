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

import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory

object FileUtils {
  /**
   * append all csv file path to a String, file path separated by comma
   */
  def getPathsFromCarbonFile(carbonFile: CarbonFile): String = {
    if (carbonFile.isDirectory) {
      val files = carbonFile.listFiles()
      val stringBuilder = new StringBuilder()
      for (j <- 0 until files.size) {
        if (files(j).getName.endsWith(".csv")) {
          stringBuilder.append(getPathsFromCarbonFile(files(j))).append(",")
        }
      }
      stringBuilder.substring(0,
        if (stringBuilder.nonEmpty) stringBuilder.size - 1 else 0)
    } else {
      carbonFile.getPath.replace('\\', '/')
    }
  }

  /**
   * append all file path to a String, inputPath path separated by comma
   *
   */
  def getPaths(inputPath: String): String = {
    if (inputPath == null || inputPath.isEmpty) {
      inputPath
    } else {
      val stringbuild = new StringBuilder()
      val filePaths = inputPath.split(",")
      for (i <- 0 until filePaths.size) {
        val fileType = FileFactory.getFileType(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), fileType)
        stringbuild.append(getPathsFromCarbonFile(carbonFile)).append(",")
      }
      stringbuild.substring(0, stringbuild.size - 1)
    }
  }

  def getSpaceOccupied(inputPath: String): Long = {
    var size : Long = 0
    if (inputPath == null || inputPath.isEmpty) {
      size
    } else {
      val stringbuild = new StringBuilder()
      val filePaths = inputPath.split(",")
      for (i <- 0 until filePaths.size) {
        val fileType = FileFactory.getFileType(filePaths(i))
        val carbonFile = FileFactory.getCarbonFile(filePaths(i), fileType)
        size = size + carbonFile.getSize
      }
      size
    }
  }
}
