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
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * This class is a Lock factory class which is used to provide lock objects.
 * Using this lock object client can request the lock and unlock.
 */
public class CarbonLockFactory {

  /**
   * Attribute for LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLockFactory.class.getName());
  /**
   * lockTypeConfigured to check if zookeeper feature is enabled or not for carbon.
   */
  private static String lockTypeConfigured;

  static {
    CarbonLockFactory.getLockTypeConfigured();
  }

  /**
   * This method will determine the lock type.
   *
   * @param absoluteTableIdentifier
   * @param lockFile
   * @return
   */
  public static ICarbonLock getCarbonLockObj(AbsoluteTableIdentifier absoluteTableIdentifier,
      String lockFile) {

    String tablePath = absoluteTableIdentifier.getTablePath();
    if (lockTypeConfigured.equals(CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER)) {
      return new ZooKeeperLocking(absoluteTableIdentifier, lockFile);
    } else if (tablePath.startsWith(CarbonCommonConstants.S3A_PREFIX) ||
        tablePath.startsWith(CarbonCommonConstants.S3N_PREFIX) ||
            tablePath.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_S3;
      return new S3FileLock(absoluteTableIdentifier, lockFile);
    } else if (tablePath.startsWith(CarbonCommonConstants.HDFSURL_PREFIX)) {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS;
      return new HdfsFileLock(absoluteTableIdentifier, lockFile);
    } else {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL;
      return new LocalFileLock(absoluteTableIdentifier, lockFile);
    }
  }

  /**
   *
   * @param locFileLocation
   * @param lockFile
   * @return carbon lock
   */
  public static ICarbonLock getCarbonLockObj(String locFileLocation, String lockFile) {
    switch (lockTypeConfigured) {
      case CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL:
        return new LocalFileLock(locFileLocation, lockFile);

      case CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER:
        return new ZooKeeperLocking(locFileLocation, lockFile);

      case CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS:
        return new HdfsFileLock(locFileLocation, lockFile);

      case CarbonCommonConstants.CARBON_LOCK_TYPE_S3:
        return new S3FileLock(locFileLocation, lockFile);

      default:
        throw new UnsupportedOperationException("Not supported the lock type");
    }
  }

  /**
   * This method will set the zookeeper status whether zookeeper to be used for locking or not.
   */
  private static void getLockTypeConfigured() {
    lockTypeConfigured = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.LOCK_TYPE_DEFAULT)
        .toUpperCase();
    LOGGER.info("Configured lock type is: " + lockTypeConfigured);
  }

}
