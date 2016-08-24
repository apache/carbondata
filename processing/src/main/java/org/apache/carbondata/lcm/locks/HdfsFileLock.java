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
package org.apache.carbondata.lcm.locks;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;

/**
 * This class is used to handle the HDFS File locking.
 * This is acheived using the concept of acquiring the data out stream using Append option.
 */
public class HdfsFileLock extends AbstractCarbonLock {

  private static final LogService LOGGER =
             LogServiceFactory.getLogService(HdfsFileLock.class.getName());
  /**
   * location hdfs file location
   */
  private String location;

  private DataOutputStream dataOutputStream;

  public static String tmpPath;

  static {
    Configuration conf = new Configuration(true);
    String hdfsPath = conf.get(CarbonCommonConstants.FS_DEFAULT_FS);
    // By default, we put the hdfs lock meta file for one table inside this table's store folder.
    // If can not get the STORE_LOCATION, then use hadoop.tmp.dir .
    tmpPath = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION,
               System.getProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION));
    if (!tmpPath.startsWith(CarbonCommonConstants.HDFSURL_PREFIX)) {
      tmpPath = hdfsPath + tmpPath;
    }
  }

  /**
   * @param lockFileLocation
   * @param lockFile
   */
  public HdfsFileLock(String lockFileLocation, String lockFile) {
    this.location = tmpPath + CarbonCommonConstants.FILE_SEPARATOR + lockFileLocation
        + CarbonCommonConstants.FILE_SEPARATOR + lockFile;
    LOGGER.info("HDFS lock path:"+this.location);
    initRetry();
  }

  /**
   * @param tableIdentifier
   * @param lockFile
   */
  public HdfsFileLock(CarbonTableIdentifier tableIdentifier, String lockFile) {
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
      return false;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#unlock()
   */
  @Override public boolean unlock() {
    if (null != dataOutputStream) {
      try {
        dataOutputStream.close();
      } catch (IOException e) {
        return false;
      } finally {
        if (FileFactory.getCarbonFile(location, FileFactory.getFileType(location)).delete()) {
          LOGGER.info("Deleted the lock file " + location);
        } else {
          LOGGER.error("Not able to delete the lock file " + location);
        }
      }
    }
    return true;
  }

}
