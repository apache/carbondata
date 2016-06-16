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
package org.carbondata.core.locks;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;

/**
 * This class is a Lock factory class which is used to provide lock objects.
 * Using this lock object client can request the lock and unlock.
 */
public class CarbonLockFactory {

  /**
   * lockTypeConfigured to check if zookeeper feature is enabled or not for carbon.
   */
  private static String lockTypeConfigured;

  static {
    CarbonLockFactory.updateZooKeeperLockingStatus();
  }

  /**
   * This method will determine the lock type.
   *
   * @param location
   * @param lockUsage
   * @return
   */
  public static ICarbonLock getCarbonLockObj(String location, LockUsage lockUsage) {
    switch (lockTypeConfigured.toUpperCase()) {
      case CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL:
        return new LocalFileLock(location, lockUsage);

      case CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER:
        return new ZooKeeperLocking(lockUsage);

      case CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS:
        return new HdfsFileLock(location, lockUsage);

      default:
        throw new UnsupportedOperationException("Not supported the lock type");
    }

  }

  /**
   * This method will set the zookeeper status whether zookeeper to be used for locking or not.
   */
  private static void updateZooKeeperLockingStatus() {
    lockTypeConfigured = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.LOCK_TYPE_DEFAULT);

  }

}
