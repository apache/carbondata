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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class handles the file locking in the local file system.
 * This will be handled using the file channel lock API.
 */
public class LocalFileLock extends AbstractCarbonLock {
  /**
   * lockFilePath is the location of the lock file.
   */
  private String lockFilePath;

  /**
   * lockFileDir is the directory of the lock file.
   */
  private String lockFileDir;

  /**
   * fileOutputStream of the local lock file
   */
  private FileOutputStream fileOutputStream;

  /**
   * channel is the FileChannel of the lock file.
   */
  private FileChannel channel;

  /**
   * fileLock NIO FileLock Object
   */
  private FileLock fileLock;

  /**
   * LOGGER for  logging the messages.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LocalFileLock.class.getName());

  /**
   * @param lockFileLocation
   * @param lockFile
   */
  public LocalFileLock(String lockFileLocation, String lockFile) {
    this.lockFileDir = CarbonTablePath.getLockFilesDirPath(lockFileLocation);
    this.lockFilePath = CarbonTablePath.getLockFilePath(lockFileLocation, lockFile);
    initRetry();
  }

  /**
   * @param tableIdentifier
   * @param lockFile
   */
  public LocalFileLock(AbsoluteTableIdentifier tableIdentifier, String lockFile) {
    this(tableIdentifier.getTablePath(), lockFile);
    initRetry();
  }

  /**
   * Lock API for locking of the file channel of the lock file.
   *
   * @return
   */
  @Override public boolean lock() {
    try {
      if (!FileFactory.isFileExist(lockFileDir)) {
        FileFactory.mkdirs(lockFileDir, FileFactory.getFileType(lockFileDir));
      }
      if (!FileFactory.isFileExist(lockFilePath)) {
        FileFactory.createNewLockFile(lockFilePath, FileFactory.getFileType(lockFilePath));
      }

      fileOutputStream = new FileOutputStream(lockFilePath);
      channel = fileOutputStream.getChannel();
      try {
        fileLock = channel.tryLock();
      } catch (OverlappingFileLockException e) {
        return false;
      }
      if (null != fileLock) {
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      LOGGER.info(e.getMessage());
      return false;
    }
  }

  /**
   * Unlock API for unlocking of the acquired lock.
   *
   * @return
   */
  @Override public boolean unlock() {
    boolean status = false;
    try {
      if (null != fileLock) {
        fileLock.release();
        status = true;
      }
    } catch (IOException e) {
      status = false;
    } finally {
      if (null != fileOutputStream) {
        try {
          fileOutputStream.close();
        } catch (IOException e) {
          LOGGER.error(e.getMessage());
        }
      }
    }
    return status;
  }
}
