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

package org.apache.carbondata.spark.util

import java.io.{BufferedWriter, File, FileFilter, FileWriter}

import scala.collection.mutable.ListBuffer

import au.com.bytecode.opencsv.CSVWriter
import org.apache.commons.io.FileUtils

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

object BadRecordUtil {

  /**
   * get the bad record redirected csv file path
   */
  def getRedirectCsvPath(dbName: String,
    tableName: String, segment: String, task: String): File = {
    var badRecordLocation = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC)
    badRecordLocation = badRecordLocation + "/" + dbName + "/" + tableName + "/" + segment + "/" +
      task
    val listFiles = new File(badRecordLocation).listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        pathname.getPath.endsWith(".csv")
      }
    })
    listFiles(0)
  }

  /**
   * compare data of csvfile and redirected csv file.
   */
  def checkRedirectedCsvContentAvailableInSource(csvFilePath: String,
    redirectCsvPath: File): Boolean = {
    val origFileLineList = FileUtils.readLines(new File(csvFilePath))
    val redirectedFileLineList = FileUtils.readLines(redirectCsvPath)
    val iterator = redirectedFileLineList.iterator()
    while (iterator.hasNext) {
      if (!origFileLineList.contains(iterator.next())) {
        return false;
      }
    }
    true
  }

  /**
   * delete the files at bad record location
   */
  def  cleanBadRecordPath(dbName: String, tableName: String): Boolean = {
    var badRecordLocation = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC)
    badRecordLocation = badRecordLocation + "/" + dbName + "/" + tableName
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(badRecordLocation))
  }

  def createCSV(rows: ListBuffer[Array[String]], csvPath: String): Unit = {
    val out = new BufferedWriter(new FileWriter(csvPath))
    val writer: CSVWriter = new CSVWriter(out, ',', CSVWriter.NO_QUOTE_CHARACTER)
    try {
      for (row <- rows) {
        writer.writeNext(row)
      }
    } finally {
      out.close()
      writer.close()
    }
  }
}
