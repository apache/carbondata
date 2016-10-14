/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.apache.carbondata.core.util.CarbonUtil;

public class BadRecordslogger {

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BadRecordslogger.class.getName());
  /**
   * Which holds the key and if any bad rec found to check from API to update
   * the status
   */
  private static Map<String, String> badRecordEntry =
      new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  /**
   * File Name
   */
  private String fileName;
  /**
   * Store path
   */
  private String storePath;
  /**
   * FileChannel
   */
  private BufferedWriter bufferedWriter;
  private DataOutputStream outStream;
  /**
   * csv file writer
   */
  private BufferedWriter bufferedCSVWriter;
  private DataOutputStream outCSVStream;
  /**
   * bad record log file path
   */
  private String logFilePath;
  /**
   * csv file path
   */
  private String csvFilePath;

  /**
   * task key which is DatabaseName/TableName/tablename
   */
  private String taskKey;

  // private final Object syncObject =new Object();

  public BadRecordslogger(String key, String fileName, String storePath) {
    // Initially no bad rec
    taskKey = key;
    this.fileName = fileName;
    this.storePath = storePath;
  }

  /**
   * @param key DatabaseName/TableName/tablename
   * @return return "Partially" and remove from map
   */
  public static String hasBadRecord(String key) {
    return badRecordEntry.remove(key);
  }

  public void addBadRecordsToBuilder(Object[] row, String reason, String valueComparer,
      boolean badRecordsLogRedirect, boolean badRecordLoggerEnable) {
    if (badRecordsLogRedirect || badRecordLoggerEnable) {
      StringBuilder logStrings = new StringBuilder();
      int size = row.length;
      int count = size;
      for (int i = 0; i < size; i++) {
        if (null == row[i]) {
          char ch =
              logStrings.length() > 0 ? logStrings.charAt(logStrings.length() - 1) : (char) -1;
          if (ch == ',') {
            logStrings = logStrings.deleteCharAt(logStrings.lastIndexOf(","));
          }
          break;
        } else if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(row[i].toString())) {
          logStrings.append(valueComparer);
        } else {
          logStrings.append(row[i]);
        }
        if (count > 1) {
          logStrings.append(',');
        }
        count--;
      }
      if (badRecordsLogRedirect) {
        writeBadRecordsToCSVFile(logStrings);
      }
      if (badRecordLoggerEnable) {
        logStrings.append("----->");
        if (null != reason) {
          if (reason.indexOf(CarbonCommonConstants.MEMBER_DEFAULT_VAL) > -1) {
            logStrings
                .append(reason.replace(CarbonCommonConstants.MEMBER_DEFAULT_VAL, valueComparer));
          } else {
            logStrings.append(reason);
          }
        }
        writeBadRecordsToFile(logStrings);
      }
    } else {
      // setting partial success entry since even if bad records are there then load
      // status should be partial success regardless of bad record logged
      badRecordEntry.put(taskKey, "Partially");
    }
  }

  /**
   *
   */
  private synchronized void writeBadRecordsToFile(StringBuilder logStrings) {
    if (null == logFilePath) {
      logFilePath =
          this.storePath + File.separator + this.fileName + CarbonCommonConstants.LOG_FILE_EXTENSION
              + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    }
    try {
      if (null == bufferedWriter) {
        FileType fileType = FileFactory.getFileType(storePath);
        if (!FileFactory.isFileExist(this.storePath, fileType)) {
          // create the folders if not exist
          FileFactory.mkdirs(this.storePath, fileType);

          // create the files
          FileFactory.createNewFile(logFilePath, fileType);
        }

        outStream = FileFactory.getDataOutputStream(logFilePath, fileType);

        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      }
      bufferedWriter.write(logStrings.toString());
      bufferedWriter.newLine();
    } catch (FileNotFoundException e) {
      LOGGER.error("Bad Log Files not found");
    } catch (IOException e) {
      LOGGER.error("Error While writing bad log File");
    } finally {
      // if the Bad record file is created means it partially success
      // if any entry present with key that means its have bad record for
      // that key
      badRecordEntry.put(taskKey, "Partially");
    }
  }

  /**
   * method will write the row having bad record in the csv file.
   *
   * @param logStrings
   */
  private synchronized void writeBadRecordsToCSVFile(StringBuilder logStrings) {
    if (null == csvFilePath) {
      csvFilePath =
          this.storePath + File.separator + this.fileName + CarbonCommonConstants.CSV_FILE_EXTENSION
              + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    }
    try {
      if (null == bufferedCSVWriter) {
        FileType fileType = FileFactory.getFileType(storePath);
        if (!FileFactory.isFileExist(this.storePath, fileType)) {
          // create the folders if not exist
          FileFactory.mkdirs(this.storePath, fileType);

          // create the files
          FileFactory.createNewFile(csvFilePath, fileType);
        }

        outCSVStream = FileFactory.getDataOutputStream(csvFilePath, fileType);

        bufferedCSVWriter = new BufferedWriter(new OutputStreamWriter(outCSVStream,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      }
      bufferedCSVWriter.write(logStrings.toString());
      bufferedCSVWriter.newLine();
    } catch (FileNotFoundException e) {
      LOGGER.error("Bad record csv Files not found");
    } catch (IOException e) {
      LOGGER.error("Error While writing bad record csv File");
    }
    finally {
      badRecordEntry.put(taskKey, "Partially");
    }
  }

  /**
   * closeStreams void
   */
  public synchronized void closeStreams() {
    CarbonUtil.closeStreams(bufferedWriter, outStream, bufferedCSVWriter, outCSVStream);
  }

}

