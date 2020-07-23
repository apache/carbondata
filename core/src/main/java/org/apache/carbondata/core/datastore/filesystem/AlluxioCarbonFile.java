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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

public class AlluxioCarbonFile extends HDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
          LogServiceFactory.getLogService(AlluxioCarbonFile.class.getName());

  public AlluxioCarbonFile(String filePath) {
    super(filePath);
  }

  public AlluxioCarbonFile(Path path) {
    super(path);
  }

  public AlluxioCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new AlluxioCarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override
  public String getAbsolutePath() {
    String absolutePath = super.getAbsolutePath();
    return getFormattedPath(absolutePath);
  }

  public static String getFormattedPath(String absolutePath) {
    if (absolutePath.startsWith("alluxio:/") && absolutePath.charAt(9) != '/') {
      return absolutePath.replace("alluxio:/", "alluxio:///");
    }
    return absolutePath;
  }

  @Override
  public boolean renameForce(String changeToName) {
    try {
      if (fileSystem instanceof DistributedFileSystem) {
        ((DistributedFileSystem) fileSystem).rename(path, new Path(changeToName),
            org.apache.hadoop.fs.Options.Rename.OVERWRITE);
        return true;
      }
      return false;
    } catch (IOException e) {
      LOGGER.error("Exception occurred: " + e.getMessage(), e);
      return false;
    }
  }

}
