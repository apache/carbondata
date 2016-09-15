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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

/**
 * This is a singleton class for initialization of zookeeper client.
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
        LOGGER.info("Table Status lock has been successfully released");
      }
    } else {
      if (locktype.equals(LockUsage.METADATA_LOCK)) {
        LOGGER.error("Not able to release the metadata lock");
      } else if (locktype.equals(LockUsage.TABLE_STATUS_LOCK)) {
        LOGGER.error("Not able to release the Table Status lock");
      }
    }
  }
}
