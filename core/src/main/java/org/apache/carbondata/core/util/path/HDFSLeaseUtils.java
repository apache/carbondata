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

package org.apache.carbondata.core.util.path;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.log4j.Logger;

/**
 * Implementation for HDFS utility methods
 */
public class HDFSLeaseUtils {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(HDFSLeaseUtils.class.getName());

  /**
   * This method will validate whether the exception thrown if for lease recovery from HDFS
   *
   * @param message
   * @return
   */
  public static boolean checkExceptionMessageForLeaseRecovery(String message) {
    // depending on the scenario few more cases can be added for validating lease recovery exception
    if (null != message && message.contains("Failed to APPEND_FILE")) {
      return true;
    }
    return false;
  }

  /**
   * This method will make attempts to recover lease on a file using the
   * distributed file system utility.
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  public static boolean recoverFileLease(String filePath) throws IOException {
    LOGGER.info("Trying to recover lease on file: " + filePath);
    FileFactory.FileType fileType = FileFactory.getFileType(filePath);
    switch (fileType) {
      case ALLUXIO:
      case HDFS:
      case S3:
        Path path = FileFactory.getPath(filePath);
        FileSystem fs = FileFactory.getFileSystem(path);
        return recoverLeaseOnFile(filePath, path, (DistributedFileSystem) fs);
      case VIEWFS:
        path = FileFactory.getPath(filePath);
        fs = FileFactory.getFileSystem(path);
        ViewFileSystem viewFileSystem = (ViewFileSystem) fs;
        Path targetFileSystemPath = viewFileSystem.resolvePath(path);
        FileSystem targetFileSystem = FileFactory.getFileSystem(targetFileSystemPath);
        if (targetFileSystem instanceof DistributedFileSystem) {
          return recoverLeaseOnFile(filePath, path, (DistributedFileSystem) targetFileSystem);
        } else {
          LOGGER.error(
              "Invalid file type. Lease recovery is not supported on filesystem with file: "
                  + filePath);
          return false;
        }
      default:
        LOGGER.error("Invalid file type. Lease recovery is not supported on filesystem with file: "
            + filePath);
        return false;
    }
  }

  /**
   * Recovers lease on a file
   *
   * @param filePath
   * @param path
   * @param fs
   * @return
   * @throws IOException
   */
  private static boolean recoverLeaseOnFile(String filePath, Path path, DistributedFileSystem fs)
      throws IOException {
    int maxAttempts = getLeaseRecoveryRetryCount();
    int retryInterval = getLeaseRecoveryRetryInterval();
    boolean leaseRecovered = false;
    IOException ioException = null;
    for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {
      try {
        leaseRecovered = fs.recoverLease(path);
        if (!leaseRecovered) {
          try {
            LOGGER.info(
                "Failed to recover lease after attempt " + retryCount + " . Will try again after "
                    + retryInterval + " ms...");
            Thread.sleep(retryInterval);
          } catch (InterruptedException e) {
            LOGGER.error(
                "Interrupted exception occurred while recovering lease for file : " + filePath, e);
          }
        }
      } catch (IOException e) {
        if (e instanceof LeaseExpiredException && e.getMessage().contains("File does not exist")) {
          LOGGER.error("The given file does not exist at path " + filePath);
          throw e;
        } else if (e instanceof FileNotFoundException) {
          LOGGER.error("The given file does not exist at path " + filePath);
          throw e;
        } else {
          LOGGER.error("Recover lease threw exception : " + e.getMessage());
          ioException = e;
        }
      }
      LOGGER.info("Retrying again after interval of " + retryInterval + " ms...");
    }
    if (leaseRecovered) {
      LOGGER.info("Successfully able to recover lease on file: " + filePath);
      return true;
    } else {
      LOGGER.error(
          "Failed to recover lease on file: " + filePath + " after retrying for " + maxAttempts
              + " at an interval of " + retryInterval);
      if (null != ioException) {
        throw ioException;
      } else {
        return false;
      }
    }
  }

  private static int getLeaseRecoveryRetryCount() {
    String retryMaxAttempts = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT,
            CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_DEFAULT);
    int retryCount = 0;
    try {
      retryCount = Integer.parseInt(retryMaxAttempts);
      if (retryCount < CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_MIN
          || retryCount > CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_MAX) {
        retryCount = Integer.parseInt(
            CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_DEFAULT);
        LOGGER.warn(
            String.format("value configured for %s is not in allowed range. Allowed range " +
                    "is >= %d and <= %d. Therefore considering default value: %d",
                CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT,
                CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_MIN,
                CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_MAX,
                retryCount
            ));
      }
    } catch (NumberFormatException ne) {
      retryCount = Integer.parseInt(
          CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT_DEFAULT);
      LOGGER.warn("value configured for " + CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_COUNT
          + " is incorrect. Therefore considering default value: " + retryCount);
    }
    return retryCount;
  }

  private static int getLeaseRecoveryRetryInterval() {
    String retryMaxAttempts = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL,
            CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_DEFAULT);
    int retryCount = 0;
    try {
      retryCount = Integer.parseInt(retryMaxAttempts);
      if (retryCount < CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_MIN
          || retryCount > CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_MAX) {
        retryCount = Integer.parseInt(
            CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_DEFAULT);
        LOGGER.warn(
            "value configured for " + CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL
                + " is not in allowed range. Allowed range is >="
                + CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_MIN + " and <="
                + CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_MAX
                + ". Therefore considering default value (ms): " + retryCount);
      }
    } catch (NumberFormatException ne) {
      retryCount = Integer.parseInt(
          CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL_DEFAULT);
      LOGGER.warn(
          "value configured for " + CarbonCommonConstants.CARBON_LEASE_RECOVERY_RETRY_INTERVAL
              + " is incorrect. Therefore considering default value (ms): " + retryCount);
    }
    return retryCount;
  }

}
