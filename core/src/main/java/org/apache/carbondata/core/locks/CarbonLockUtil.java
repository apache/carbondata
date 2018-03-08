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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class contains all carbon lock utilities
 */
public class CarbonLockUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLockUtil.class.getName());

  /**
   * unlocks given file
   *
   * @param carbonLock
   */
  public static void fileUnlock(ICarbonLock carbonLock, String locktype) {
    if (carbonLock.unlock()) {
      if (locktype.equals(LockUsage.METADATA_LOCK)) {
        LOGGER.info("Metadata lock has been successfully released");
      } else if (locktype.equals(LockUsage.TABLE_STATUS_LOCK)) {
        LOGGER.info("Table status lock has been successfully released");
      }
      else if (locktype.equals(LockUsage.CLEAN_FILES_LOCK)) {
        LOGGER.info("Clean files lock has been successfully released");
      }
      else if (locktype.equals(LockUsage.DELETE_SEGMENT_LOCK)) {
        LOGGER.info("Delete segments lock has been successfully released");
      }
    } else {
      if (locktype.equals(LockUsage.METADATA_LOCK)) {
        LOGGER.error("Not able to release the metadata lock");
      } else if (locktype.equals(LockUsage.TABLE_STATUS_LOCK)) {
        LOGGER.error("Not able to release the table status lock");
      }
      else if (locktype.equals(LockUsage.CLEAN_FILES_LOCK)) {
        LOGGER.info("Not able to release the clean files lock");
      }
      else if (locktype.equals(LockUsage.DELETE_SEGMENT_LOCK)) {
        LOGGER.info("Not able to release the delete segments lock");
      }
    }
  }

  /**
   * Given a lock type this method will return a new lock object if not acquired by any other
   * operation
   *
   * @param absoluteTableIdentifier
   * @param lockType
   * @return
   */
  public static ICarbonLock getLockObject(AbsoluteTableIdentifier absoluteTableIdentifier,
      String lockType, String errorMsg) {
    ICarbonLock carbonLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier, lockType);
    LOGGER.info("Trying to acquire lock: " + lockType + "for table: " +
        absoluteTableIdentifier.toString());
    if (carbonLock.lockWithRetries()) {
      LOGGER.info("Successfully acquired the lock " + lockType + "for table: " +
          absoluteTableIdentifier.toString());
    } else {
      LOGGER.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return carbonLock;
  }

  /**
   * Get and lock with default error message
   */
  public static ICarbonLock getLockObject(AbsoluteTableIdentifier identifier, String lockType) {
    return getLockObject(identifier,
        lockType,
        "Acquire table lock failed after retry, please try after some time");
  }

  /**
   * Get the value for the property. If NumberFormatException is thrown then return default value.
   */
  public static int getLockProperty(String property, int defaultValue) {
    try {
      return Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(property));
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Currently the segment lock files are not deleted immediately when unlock,
   * so it needs to delete expired lock files before delete loads.
   */
  public static void deleteExpiredSegmentLockFiles(CarbonTable carbonTable) {
    final long currTime = System.currentTimeMillis();
    final long segmentLockFilesPreservTime =
        CarbonProperties.getInstance().getSegmentLockFilesPreserveHours();
    AbsoluteTableIdentifier absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    String lockFilesDir = CarbonTablePath
        .getLockFilesDirPath(absoluteTableIdentifier.getTablePath());
    CarbonFile[] files = FileFactory.getCarbonFile(lockFilesDir)
        .listFiles(new CarbonFileFilter() {

            @Override public boolean accept(CarbonFile pathName) {
              if (CarbonTablePath.isSegmentLockFilePath(pathName.getName())) {
                if ((currTime - pathName.getLastModifiedTime()) > segmentLockFilesPreservTime) {
                  return true;
                }
              }
              return false;
            }
        }
    );

    for (CarbonFile file : files) {
      file.delete();
    }
  }
}
