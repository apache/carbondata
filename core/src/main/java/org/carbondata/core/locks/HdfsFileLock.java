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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.carbondata.core.datastorage.store.impl.FileFactory;

/**
 * This class is used to handle the HDFS File locking.
 * This is acheived using the concept of acquiring the data out stream using Append option.
 */
public class HdfsFileLock extends AbstractCarbonLock {

  /**
   * location hdfs file location
   */
  private String location;

  /**
   * lockUsage is used to determine the type of the lock. according to this the lock
   * folder will change.
   */
  private LockUsage lockUsage;

  private DataOutputStream dataOutputStream;

  /**
   * @param location
   * @param lockUsage
   */
  public HdfsFileLock(String location, LockUsage lockUsage) {
    this.location = location;
    this.lockUsage = lockUsage;
    this.location = location + File.separator + this.lockUsage;
    initRetry();
  }

  /* (non-Javadoc)
   * @see org.carbondata.core.locks.ICarbonLock#lock()
   */
  @Override public boolean lock() {
    try {
      if (!FileFactory.isFileExist(location, FileFactory.getFileType(location))) {
        FileFactory.createNewLockFile(location, FileFactory.getFileType(location));
      }
      dataOutputStream =
          FileFactory.getDataOutputStreamUsingAppend(location, FileFactory.getFileType(location));

      return true;

    } catch (IOException e) {
      return false;
    }
  }

  /* (non-Javadoc)
   * @see org.carbondata.core.locks.ICarbonLock#unlock()
   */
  @Override public boolean unlock() {
    if (null != dataOutputStream) {
      try {
        dataOutputStream.close();
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

}
