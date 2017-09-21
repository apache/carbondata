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

package org.apache.carbondata.core.locks;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;


/**
 * This class is used to handle the HDFS File locking.
 * This is acheived using the concept of acquiring the data out stream using Append option.
 */
public class S3FileLock extends AbstractCarbonLock {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(S3FileLock.class.getName());
  /**
   * location hdfs file location
   */
  private String location;

  private DataOutputStream dataOutputStream;

  private static String tmpPath;

  static {
    Configuration conf = new Configuration(true);
    String s3Path = conf.get(CarbonCommonConstants.FS_DEFAULT_FS);
    // By default, we put the s3 lock meta file for one table inside this table's store folder.
    // If can not get the STORE_LOCATION, then use hadoop.tmp.dir .
    tmpPath = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION,
        System.getProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION));
    if (!tmpPath.startsWith(CarbonCommonConstants.S3URL_PREFIX)) {
      tmpPath = s3Path + tmpPath;
    }
  }

  /**
   * @param lockFileLocation
   * @param lockFile
   */
  public S3FileLock(String lockFileLocation, String lockFile) {
    this.location = tmpPath + CarbonCommonConstants.FILE_SEPARATOR + lockFileLocation
        + CarbonCommonConstants.FILE_SEPARATOR + lockFile;
    LOGGER.info("S3 lock path:" + this.location);
    initRetry();
  }

  /**
   * @param lockFilePath
   */
  public S3FileLock(String lockFilePath) {
    this.location = lockFilePath;
    initRetry();
  }

  /**
   * @param tableIdentifier
   * @param lockFile
   */
  public S3FileLock(CarbonTableIdentifier tableIdentifier, String lockFile) {
    this(tableIdentifier.getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR + tableIdentifier
        .getTableName(), lockFile);
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#lock()
   */
  @Override public boolean lock() {
    try {
      if (!FileFactory.isFileExist(location, FileFactory.getFileType(location))) {
        FileFactory.createNewLockFile(location, FileFactory.getFileType(location));
      }
      dataOutputStream =
          FileFactory.getDataOutputStreamUsingAppend(location, FileFactory.getFileType(location));

      return true;

    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return false;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#unlock()
   */
  @Override
  public boolean unlock() {
    boolean status = false;
    if (null != dataOutputStream) {
      try {
        dataOutputStream.close();
        status = true;
      } catch (IOException e) {
        status = false;
      } finally {
        CarbonFile carbonFile =
            FileFactory.getCarbonFile(location, FileFactory.getFileType(location));
        if (carbonFile.exists()) {
          if (carbonFile.delete()) {
            LOGGER.info("Deleted the lock file " + location);
          } else {
            LOGGER.error("Not able to delete the lock file " + location);
            status = false;
          }
        } else {
          LOGGER.error("Not able to delete the lock file because "
              + "it is not existed in location " + location);
          status = false;
        }
      }
    }
    return status;
  }

}
