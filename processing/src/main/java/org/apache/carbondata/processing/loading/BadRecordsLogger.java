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

package org.apache.carbondata.processing.loading;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import org.apache.log4j.Logger;

public class BadRecordsLogger {

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(BadRecordsLogger.class.getName());
  /**
   * Which holds the key and if any bad rec found to check from API to update
   * the status
   */
  private static Map<String, String> badRecordEntry =
      new ConcurrentHashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
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

  private boolean badRecordsLogRedirect;

  private boolean badRecordLoggerEnable;

  private boolean badRecordConvertNullDisable;

  private boolean isDataLoadFail;

  private boolean isCompactionFlow;

  // private final Object syncObject =new Object();

  public BadRecordsLogger(String key, String fileName, String storePath,
      boolean badRecordsLogRedirect, boolean badRecordLoggerEnable,
      boolean badRecordConvertNullDisable, boolean isDataLoadFail, boolean isCompactionFlow) {
    // Initially no bad rec
    taskKey = key;
    this.fileName = fileName;
    this.storePath = storePath;
    this.badRecordsLogRedirect = badRecordsLogRedirect;
    this.badRecordLoggerEnable = badRecordLoggerEnable;
    this.badRecordConvertNullDisable = badRecordConvertNullDisable;
    this.isDataLoadFail = isDataLoadFail;
    this.isCompactionFlow = isCompactionFlow;
  }

  public boolean isCompFlow() {
    return isCompactionFlow;
  }

  /**
   * @param key DatabaseNaame/TableName/tablename
   * @return return "Partially"
   */
  public static String hasBadRecord(String key) {
    return badRecordEntry.get(key);
  }

  /**
   * @param key DatabaseNaame/TableName/tablename
   * @return remove key from the map
   */
  public static String removeBadRecordKey(String key) {
    return badRecordEntry.remove(key);
  }

  public void addBadRecordsToBuilder(Object[] row, String reason)
      throws CarbonDataLoadingException {
    // setting partial success entry since even if bad records are there then load
    // status should be partial success regardless of bad record logged
    badRecordEntry.put(taskKey, "Partially");
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
          logStrings.append("null");
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
                .append(reason.replace(CarbonCommonConstants.MEMBER_DEFAULT_VAL, "null"));
          } else {
            logStrings.append(reason);
          }
        }
        writeBadRecordsToFile(logStrings);
      }
    }
  }

  /**
   *
   */
  private synchronized void writeBadRecordsToFile(StringBuilder logStrings)
      throws CarbonDataLoadingException {
    if (null == logFilePath) {
      logFilePath =
          this.storePath + File.separator + this.fileName + CarbonCommonConstants.LOG_FILE_EXTENSION
              + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    }
    try {
      if (null == bufferedWriter) {
        if (!FileFactory.isFileExist(this.storePath)) {
          // create the folders if not exist
          FileFactory.mkdirs(this.storePath);

          // create the files
          FileFactory.createNewFile(logFilePath);
        }

        outStream = FileFactory.getDataOutputStream(logFilePath);

        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      }
      bufferedWriter.write(logStrings.toString());
      bufferedWriter.newLine();
    } catch (FileNotFoundException e) {
      LOGGER.error("Bad Log Files not found");
      throw new CarbonDataLoadingException("Bad Log Files not found", e);
    } catch (IOException e) {
      LOGGER.error("Error While writing bad record log File");
      throw new CarbonDataLoadingException("Error While writing bad record log File", e);
    }
  }

  /**
   * method will write the row having bad record in the csv file.
   *
   * @param logStrings
   */
  private synchronized void writeBadRecordsToCSVFile(StringBuilder logStrings)
      throws CarbonDataLoadingException {
    if (null == csvFilePath) {
      csvFilePath =
          this.storePath + File.separator + this.fileName + CarbonCommonConstants.CSV_FILE_EXTENSION
              + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    }
    try {
      if (null == bufferedCSVWriter) {
        if (!FileFactory.isFileExist(this.storePath)) {
          // create the folders if not exist
          FileFactory.mkdirs(this.storePath);

          // create the files
          FileFactory.createNewFile(csvFilePath);
        }

        outCSVStream = FileFactory.getDataOutputStream(csvFilePath);

        bufferedCSVWriter = new BufferedWriter(new OutputStreamWriter(outCSVStream,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      }
      bufferedCSVWriter.write(logStrings.toString());
      bufferedCSVWriter.newLine();
    } catch (FileNotFoundException e) {
      LOGGER.error("Bad record csv Files not found");
      throw new CarbonDataLoadingException("Bad record csv Files not found", e);
    } catch (IOException e) {
      LOGGER.error("Error While writing bad record csv File");
      throw new CarbonDataLoadingException("Error While writing bad record csv File", e);
    }
  }

  public boolean isBadRecordConvertNullDisable() {
    return badRecordConvertNullDisable;
  }

  public boolean isDataLoadFail() {
    return isDataLoadFail;
  }

  public boolean isBadRecordLoggerEnable() {
    return badRecordLoggerEnable;
  }

  public boolean isBadRecordsLogRedirect() {
    return badRecordsLogRedirect;
  }

  /**
   * closeStreams void
   */
  public synchronized void closeStreams() {
    // removing taskKey Entry while closing the stream
    // This will make sure the cleanup of the task status even in case of some failure.
    removeBadRecordKey(taskKey);
    CarbonUtil.closeStreams(bufferedWriter, outStream, bufferedCSVWriter, outCSVStream);
  }

}

