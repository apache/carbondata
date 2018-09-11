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
import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class is used to handle the S3 File locking.
 * This is acheived using the concept of acquiring the data out stream using Append option.
 */
public class S3FileLock extends AbstractCarbonLock {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(S3FileLock.class.getName());

  /**
   * lockFileDir is the directory of the lock file.
   */
  private String lockFileDir;

  private DataOutputStream dataOutputStream;

  /**
   * @param tableIdentifier
   * @param lockFile
   */
  public S3FileLock(AbsoluteTableIdentifier tableIdentifier, String lockFile) {
    this(tableIdentifier.getTablePath(), lockFile);
  }

  /**
   * @param lockFileLocation
   * @param lockFile
   */
  public S3FileLock(String lockFileLocation, String lockFile) {
    this.lockFileDir = CarbonTablePath.getLockFilesDirPath(lockFileLocation);
    this.lockFilePath = CarbonTablePath.getLockFilePath(lockFileLocation, lockFile);
    LOGGER.info("S3 lock path:" + this.lockFilePath);
    initRetry();
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#unlock()
   */
  @Override public boolean unlock() {
    boolean status = false;
    if (null != dataOutputStream) {
      try {
        dataOutputStream.close();
        status = true;
      } catch (IOException e) {
        status = false;
      }
    }
    return status;
  }

  /* (non-Javadoc)
   * @see org.apache.carbondata.core.locks.ICarbonLock#lock()
   */
  @Override public boolean lock() {
    try {
      if (!FileFactory.isFileExist(lockFileDir)) {
        FileFactory.mkdirs(lockFileDir, FileFactory.getFileType(lockFileDir));
      }
      if (!FileFactory.isFileExist(lockFilePath)) {
        FileFactory.createNewLockFile(lockFilePath, FileFactory.getFileType(lockFilePath));
      }
      dataOutputStream =
          FileFactory.getDataOutputStreamUsingAppend(lockFilePath,
              FileFactory.getFileType(lockFilePath));
      return true;
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
      return false;
    }
  }

}
