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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * This is the abstract class of the lock implementations.This handles the
 * retrying part of the locking.
 */
public abstract class AbstractCarbonLock implements ICarbonLock {
  private int retryCount;

  private int retryTimeout;

  /**
   * lockFilePath is the location of the lock file.
   */
  protected String lockFilePath;

  public abstract boolean lock();

  @Override
  public String getLockFilePath() {
    return this.lockFilePath;
  }

  /**
   * API for enabling the locking of file with retries.
   */
  public boolean lockWithRetries() {
    try {
      for (int i = 0; i < retryCount; i++) {
        if (lock()) {
          return true;
        } else {
          Thread.sleep(retryTimeout * 1000L);
        }
      }
    } catch (InterruptedException e) {
      return false;
    }
    return false;
  }

  /**
   * API for enabling the locking of file with retries.
   */
  public boolean lockWithRetries(int retries, int retryInterval) {
    retryCount = retries;
    retryTimeout = retryInterval;
    return lockWithRetries();
  }

  /**
   * Initializes the retry count and retry timeout.
   * This will determine how many times to retry to acquire lock and the retry timeout.
   */
  protected void initRetry() {
    String retries = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK);
    try {
      retryCount = Integer.parseInt(retries);
    } catch (NumberFormatException e) {
      retryCount = CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK_DEFAULT;
    }

    String maxTimeout = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CARBON_LOCK);
    try {
      retryTimeout = Integer.parseInt(maxTimeout);
    } catch (NumberFormatException e) {
      retryTimeout = CarbonCommonConstants.MAX_TIMEOUT_FOR_CARBON_LOCK_DEFAULT;
    }

  }

  public boolean releaseLockManually(String lockFile) {
    try {
      return FileFactory.deleteFile(lockFile, FileFactory.getFileType(lockFile));
    } catch (Exception e) {
      return false;
    }
  }

}
