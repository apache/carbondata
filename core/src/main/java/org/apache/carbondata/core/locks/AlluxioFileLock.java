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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

import org.apache.log4j.Logger;

/**
 * This class is used to handle the Alluxio File locking.
 * This is acheived using the concept of acquiring the data out stream using Append option.
 */
public class AlluxioFileLock extends HdfsFileLock {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
          LogServiceFactory.getLogService(AlluxioFileLock.class.getName());
  /**
   * lockFilePath is the location of the lock file.
   */
  private String lockFilePath;

  /**
   * lockFileDir is the directory of the lock file.
   */
  private String lockFileDir;

  private DataOutputStream dataOutputStream;

  /**
   * @param tableIdentifier
   * @param lockFile
   */
  public AlluxioFileLock(AbsoluteTableIdentifier tableIdentifier, String lockFile) {
    this(tableIdentifier.getTablePath(), lockFile);
  }

  /**
   * @param lockFileLocation
   * @param lockFile
   */
  public AlluxioFileLock(String lockFileLocation, String lockFile) {
    super(lockFileLocation, lockFile);
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#unlock()
   */
  @Override public boolean unlock() {
    return super.unlock();
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#lock()
   */
  @Override public boolean lock() {
    return super.lock();
  }
}
