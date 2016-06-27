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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory;

/**
 * This class handles the file locking in the local file system.
 * This will be handled using the file channel lock API.
 */
public class LocalFileLock extends AbstractCarbonLock {
  /**
   * location is the location of the lock file.
   */
  private String location;

  /**
   * lockUsage will determine the lock folder. so that similar locks will try to acquire
   * same lock file.
   */
  private LockUsage lockUsage;

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

  public static final String tmpPath;

  private String cubeName;

  private String schemaName;

  /**
   * LOGGER for  logging the messages.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LocalFileLock.class.getName());

  static {
    tmpPath = System.getProperty("java.io.tmpdir");
  }

  /**
   * @param location
   * @param lockUsage
   */
  public LocalFileLock(String location, LockUsage lockUsage) {
    this.lockUsage = lockUsage;
    location = location.replace("\\", "/");
    String tempStr = location.substring(0, location.lastIndexOf('/'));
    cubeName = tempStr.substring(tempStr.lastIndexOf('/') + 1, tempStr.length());
    tempStr = tempStr.substring(0, tempStr.lastIndexOf('/'));
    schemaName = tempStr.substring(tempStr.lastIndexOf('/') + 1, tempStr.length());
    this.location =
        tmpPath + File.separator + schemaName + File.separator + cubeName + File.separator
            + this.lockUsage;
    initRetry();
  }

  /**
   * Lock API for locking of the file channel of the lock file.
   *
   * @return
   */
  @Override public boolean lock() {
    try {
      String schemaFolderPath = tmpPath + File.separator + schemaName;
      String cubeFolderPath = schemaFolderPath + File.separator + cubeName;
      // create dir with schema name in tmp location.
      if (!FileFactory.isFileExist(schemaFolderPath, FileFactory.getFileType(tmpPath))) {
        FileFactory.mkdirs(schemaFolderPath, FileFactory.getFileType(tmpPath));
      }

      // create dir with cube name in tmp location.
      if (!FileFactory.isFileExist(cubeFolderPath, FileFactory.getFileType(tmpPath))) {
        FileFactory.mkdirs(cubeFolderPath, FileFactory.getFileType(tmpPath));
      }
      if (!FileFactory.isFileExist(location, FileFactory.getFileType(location))) {
        FileFactory.createNewLockFile(location, FileFactory.getFileType(location));
      }

      fileOutputStream = new FileOutputStream(location);
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
      return false;
    }

  }

  /**
   * Unlock API for unlocking of the acquired lock.
   *
   * @return
   */
  @Override public boolean unlock() {
    boolean status;
    try {
      if (null != fileLock) {
        fileLock.release();
      }
      status = true;
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
