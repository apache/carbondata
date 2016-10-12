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

package org.apache.carbondata.processing.util;

import java.io.File;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;


public class LocalDirectoryChooser {
  private static final LogService LOGGER =
            LogServiceFactory.getLogService(LocalDirectoryChooser.class.getName());
  private String[] localDirs = null;
  private int length = 0;
  private int index = 0;

  private static LocalDirectoryChooser chooser = null;

  private LocalDirectoryChooser(String[] localDirs) {
    this.init(localDirs);
  }

  private void init(String[] localDirs) {
    this.localDirs = localDirs;
    this.index = -1;
    this.length = localDirs.length;
  }

  public static LocalDirectoryChooser newInstance(String[] localDirs) {
    chooser = new LocalDirectoryChooser(localDirs);
    return chooser;
  }

  public static LocalDirectoryChooser getInstance() {
    return chooser;
  }

  private synchronized  int nextIndex(){
    index += 1;
    if (index == length) {
      index = 0;
    }
    return index;
  }

  public String nextLocalDir() {
    return localDirs[nextIndex()];
  }

  public String[] getLocalDirs() {
    return this.localDirs;
  }

  public static String getLocalDirectory(CarbonTable carbonTable, String tempFolder, String
          taskNo, String partitionID, String segmentId) {
    String baseStoreLocation = LocalDirectoryChooser.getInstance().nextLocalDir();
    baseStoreLocation = baseStoreLocation + File.separator + tempFolder +  File.separator +
        taskNo;
    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(baseStoreLocation,
        carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath = carbonTablePath.getCarbonDataDirectoryPath(partitionID,
        segmentId);
    String localDirectory = carbonDataDirectoryPath + File.separator + taskNo +
        File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    if (!new File(localDirectory).mkdirs()) {
      LOGGER.info("Sort Temp Location Already Exists");
    }
    return localDirectory;
  }

}
