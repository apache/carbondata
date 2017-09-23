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
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

import org.apache.hadoop.conf.Configuration;

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
   * @param identifier
   * @param lockType
   * @return
   */
  public static ICarbonLock getLockObject(CarbonTableIdentifier identifier, String lockType,
      Configuration configuration) {
    ICarbonLock carbonLock =
        CarbonLockFactory.getCarbonLockObj(identifier, lockType, configuration);
    LOGGER.info("Trying to acquire lock: " + carbonLock);
    if (carbonLock.lockWithRetries()) {
      LOGGER.info("Successfully acquired the lock " + carbonLock);
    } else {
      throw new RuntimeException("Table is locked for updation. Please try after some time");
    }
    return carbonLock;
  }

}
