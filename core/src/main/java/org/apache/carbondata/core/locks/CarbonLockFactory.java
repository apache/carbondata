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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

/**
 * This class is a Lock factory class which is used to provide lock objects.
 * Using this lock object client can request the lock and unlock.
 */
public class CarbonLockFactory {

  /**
   * Attribute for LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonLockFactory.class.getName());
  /**
   * lockTypeConfigured to check if zookeeper feature is enabled or not for carbon.
   */
  private static String lockTypeConfigured;

  private static String lockPath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.LOCK_PATH, CarbonCommonConstants.LOCK_PATH_DEFAULT)
      .toLowerCase();

  private static Constructor lockConstructor;

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
    String absoluteLockPath;
    if (lockPath.isEmpty()) {
      absoluteLockPath = absoluteTableIdentifier.getTablePath();
    } else {
      absoluteLockPath =
          getLockPath(absoluteTableIdentifier.getCarbonTableIdentifier().getTableId());
    }
    FileFactory.FileType fileType = FileFactory.getFileType(absoluteLockPath);
    if (lockTypeConfigured.equals(CarbonCommonConstants.CARBON_LOCK_TYPE_CUSTOM)) {
      return newCustomLock(absoluteLockPath, lockFile);
    } else if (lockTypeConfigured.equals(CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER)) {
      return new ZooKeeperLocking(absoluteLockPath, lockFile);
    } else if (fileType == FileFactory.FileType.S3) {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_S3;
      return new S3FileLock(absoluteLockPath,
                lockFile);
    } else if (fileType == FileFactory.FileType.HDFS || fileType == FileFactory.FileType.VIEWFS) {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS;
      return new HdfsFileLock(absoluteLockPath, lockFile);
    } else if (fileType == FileFactory.FileType.ALLUXIO) {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_ALLUXIO;
      return new AlluxioFileLock(absoluteLockPath, lockFile);
    } else {
      lockTypeConfigured = CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL;
      return new LocalFileLock(absoluteLockPath, lockFile);
    }
  }

  /**
   *
   * @param locFileLocation
   * @param lockFile
   * @return carbon lock
   */
  public static ICarbonLock getSystemLevelCarbonLockObj(String locFileLocation, String lockFile) {
    String lockFileLocation;
    if (lockPath.isEmpty()) {
      lockFileLocation = locFileLocation;
    } else {
      lockFileLocation = getLockPath("1");
    }
    switch (lockTypeConfigured) {
      case CarbonCommonConstants.CARBON_LOCK_TYPE_CUSTOM:
        return newCustomLock(lockFileLocation, lockFile);
      case CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL:
        return new LocalFileLock(lockFileLocation, lockFile);
      case CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER:
        return new ZooKeeperLocking(lockFileLocation, lockFile);
      case CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS:
        return new HdfsFileLock(lockFileLocation, lockFile);
      case CarbonCommonConstants.CARBON_LOCK_TYPE_S3:
        return new S3FileLock(lockFileLocation, lockFile);
      case CarbonCommonConstants.CARBON_LOCK_TYPE_ALLUXIO:
        return new AlluxioFileLock(lockFileLocation, lockFile);
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
    String lockClassName =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.LOCK_CLASS);
    if (lockClassName == null) {
      return;
    }
    CarbonLockFactory.lockConstructor = getCustomLockConstructor(lockClassName);
  }

  public static String getLockPath(String tableId) {
    return lockPath + CarbonCommonConstants.FILE_SEPARATOR + tableId;
  }

  private static ICarbonLock newCustomLock(final String lockFileLocation, final String lockFile) {
    if (lockConstructor == null) {
      throw new IllegalArgumentException(
          "Carbon property [" + CarbonCommonConstants.LOCK_CLASS + "] is not set.");
    }
    try {
      return (ICarbonLock) lockConstructor.newInstance(lockFileLocation, lockFile);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static Constructor<?> getCustomLockConstructor(final String lockClassName) {
    final Class<?> lockClass;
    try {
      lockClass = CarbonLockFactory.class.getClassLoader().loadClass(lockClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("The class [" + lockClassName + "] is not found.");
    }
    if (!ICarbonLock.class.isAssignableFrom(lockClass)) {
      throw new IllegalArgumentException(
          "The class [" + lockClassName + "] is not an ICarbonLock class.");
    }
    if (Modifier.isAbstract(lockClass.getModifiers())) {
      throw new IllegalArgumentException(
          "The class [" + lockClassName + "] can not be initialized.");
    }
    if (!Modifier.isPublic(lockClass.getModifiers())) {
      throw new IllegalArgumentException(
          "The class [" + lockClassName + "] is not a public class.");
    }
    final Constructor<?> lockConstructor;
    try {
      lockConstructor = lockClass.getConstructor(String.class, String.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "The class [" + lockClassName + "] do not have the constructor(String, String).", e
      );
    }
    if (!Modifier.isPublic(lockClass.getModifiers())) {
      throw new IllegalArgumentException(
          "The constructor [" + lockConstructor + "] is not a public constructor.");
    }
    return lockConstructor;
  }
}
