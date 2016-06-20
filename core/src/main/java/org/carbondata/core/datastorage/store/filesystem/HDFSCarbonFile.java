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

package org.carbondata.core.datastorage.store.filesystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HDFSCarbonFile extends AbstractDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(HDFSCarbonFile.class.getName());

  public HDFSCarbonFile(String filePath) {
    super(filePath);
  }

  public HDFSCarbonFile(Path path) {
    super(path);
  }

  public HDFSCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  /**
   * @param listStatus
   * @return
   */
  private CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new HDFSCarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override
  public CarbonFile[] listFiles() {
    FileStatus[] listStatus = null;
    try {
      if (null != fileStatus && fileStatus.isDirectory()) {
        Path path = fileStatus.getPath();
        listStatus = path.getFileSystem(FileFactory.getConfiguration()).listStatus(path);
      } else {
        return null;
      }
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return new CarbonFile[0];
    }
    return getFiles(listStatus);
  }

  @Override
  public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    CarbonFile[] files = listFiles();
    if (files != null && files.length >= 1) {
      List<CarbonFile> fileList = new ArrayList<CarbonFile>(files.length);
      for (int i = 0; i < files.length; i++) {
        if (fileFilter.accept(files[i])) {
          fileList.add(files[i]);
        }
      }
      if (fileList.size() >= 1) {
        return fileList.toArray(new CarbonFile[fileList.size()]);
      } else {
        return new CarbonFile[0];
      }
    }
    return files;
  }

  @Override
  public CarbonFile getParentFile() {
    return new HDFSCarbonFile(fileStatus.getPath().getParent());
  }

  @Override
  public boolean renameForce(String changetoName) {
    FileSystem fs;
    try {
      fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
      if (fs instanceof DistributedFileSystem) {
        ((DistributedFileSystem) fs).rename(fileStatus.getPath(), new Path(changetoName),
            org.apache.hadoop.fs.Options.Rename.OVERWRITE);
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }
}