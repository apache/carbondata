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

package org.apache.carbondata.core.datastore.impl;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.AlluxioCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.HDFSCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.S3CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.ViewFSCarbonFile;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * FileType provider to create CarbonFile specific to the file system where the path belongs to.
 */
public class DefaultFileTypeProvider implements FileTypeInterface {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DefaultFileTypeProvider.class.getName());

  /**
   * Custom file type provider for supporting non default file systems.
   */
  protected FileTypeInterface customFileTypeProvider = null;

  protected boolean customFileTypeProviderInitialized = false;

  public DefaultFileTypeProvider() {
  }

  /**
   * This method is required apart from Constructor to handle the below circular dependency.
   * CarbonProperties-->FileFactory-->DefaultTypeProvider-->CarbonProperties
   */
  private void initializeCustomFileprovider() {
    if (!customFileTypeProviderInitialized) {
      customFileTypeProviderInitialized = true;
      String customFileProvider =
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CUSTOM_FILE_PROVIDER);
      if (customFileProvider != null && !customFileProvider.trim().isEmpty()) {
        try {
          customFileTypeProvider =
              (FileTypeInterface) Class.forName(customFileProvider).newInstance();
        } catch (Exception e) {
          LOGGER.error("Unable load configured FileTypeInterface class. Ignored.", e);
        }
      }
    }
  }

  /**
   * Delegate to the custom file provider to check if the path is supported or not.
   * Note this function do not check the default supported file systems as  #getCarbonFile expects
   * this method output is from customFileTypeProvider.
   *
   * @param path path of the file
   * @return true if supported by the custom
   */
  @Override public boolean isPathSupported(String path) {
    initializeCustomFileprovider();
    if (customFileTypeProvider != null) {
      return customFileTypeProvider.isPathSupported(path);
    }
    return false;
  }

  public CarbonFile getCarbonFile(String path, Configuration conf) {
    // Handle the custom file type first
    if (isPathSupported(path)) {
      return customFileTypeProvider.getCarbonFile(path, conf);
    }

    FileFactory.FileType fileType = FileFactory.getFileType(path);
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
      case HDFS:
        return new HDFSCarbonFile(path, conf);
      case S3:
        return new S3CarbonFile(path, conf);
      case ALLUXIO:
        return new AlluxioCarbonFile(path);
      case VIEWFS:
        return new ViewFSCarbonFile(path);
      default:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
    }
  }
}
