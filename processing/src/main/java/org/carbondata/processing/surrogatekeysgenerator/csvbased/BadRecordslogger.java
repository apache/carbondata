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

package org.carbondata.processing.surrogatekeysgenerator.csvbased;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.util.CarbonUtil;

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
   *
   */
  private CarbonFile logFile;
  /**
   * task key which is Schemaname/CubeName/tablename
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
   * @param key Schemaname/CubeName/tablename
   * @return return "Partially" and remove from map
   */
  public static String hasBadRecord(String key) {
    return badRecordEntry.remove(key);
  }

  public void addBadRecordsToBilder(Object[] row, int size, String reason, String valueComparer) {
    StringBuilder logStrings = new StringBuilder();
    int count = size;
    for (int i = 0; i < size; i++) {
      if (null == row[i]) {
        logStrings.append(row[i]);
      } else if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(row[i].toString())) {
        logStrings.append(valueComparer);
      } else {
        logStrings.append(row[i]);
      }
      if (count > 1) {
        logStrings.append(" , ");
      }
      count--;
    }

    logStrings.append("----->");
    if (null != reason) {
      if (reason.indexOf(CarbonCommonConstants.MEMBER_DEFAULT_VAL) > -1) {
        logStrings.append(reason.replace(CarbonCommonConstants.MEMBER_DEFAULT_VAL, valueComparer));
      } else {
        logStrings.append(reason);
      }
    }

    writeBadRecordsToFile(logStrings);
  }

  /**
   *
   */
  private synchronized void writeBadRecordsToFile(StringBuilder logStrings) {
    String filePath = this.storePath + File.separator + this.fileName
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    if (null == logFile) {
      logFile = FileFactory.getCarbonFile(filePath, FileFactory.getFileType(filePath));
    }

    try {
      if (null == bufferedWriter) {
        FileType fileType = FileFactory.getFileType(storePath);
        if (!FileFactory.isFileExist(this.storePath, fileType)) {
          // create the folders if not exist
          FileFactory.mkdirs(this.storePath, fileType);

          // create the files
          FileFactory.createNewFile(filePath, fileType);
        }

        outStream = FileFactory.getDataOutputStream(filePath, fileType);

        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream,
            CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT));

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
   * closeStreams void
   */
  public synchronized void closeStreams() {
    CarbonUtil.closeStreams(bufferedWriter, outStream);
  }

}

