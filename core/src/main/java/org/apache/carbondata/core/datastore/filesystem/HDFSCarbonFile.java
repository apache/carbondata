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

package org.apache.carbondata.core.datastore.filesystem;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

public class HDFSCarbonFile extends AbstractDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(HDFSCarbonFile.class.getName());

  public HDFSCarbonFile(String filePath) {
    super(filePath);
  }

  public HDFSCarbonFile(String filePath, Configuration hadoopConf) {
    super(filePath, hadoopConf);
  }

  public HDFSCarbonFile(Path path) {
    super(path);
  }

  public HDFSCarbonFile(Path path, Configuration hadoopConf) {
    super(path, hadoopConf);
  }

  public HDFSCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  /**
   * @param listStatus
   * @return
   */
  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
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
  public boolean renameForce(String changeToName) {
    try {
      if (fileSystem instanceof DistributedFileSystem) {
        ((DistributedFileSystem) fileSystem).rename(path, new Path(changeToName),
            org.apache.hadoop.fs.Options.Rename.OVERWRITE);
        return true;
      } else if ((fileSystem instanceof FilterFileSystem) && (((FilterFileSystem) fileSystem)
          .getRawFileSystem() instanceof DistributedFileSystem)) {
        ((DistributedFileSystem) ((FilterFileSystem) fileSystem).getRawFileSystem())
            .rename(path, new Path(changeToName), org.apache.hadoop.fs.Options.Rename.OVERWRITE);
        return true;
      } else {
        return fileSystem.rename(path, new Path(changeToName));
      }
    } catch (IOException e) {
      LOGGER.error("Exception occurred: " + e.getMessage(), e);
      return false;
    }
  }
}