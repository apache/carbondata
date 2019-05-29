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

package org.apache.carbondata.core.carbon.datastorage.filesystem.store.impl;

import org.apache.carbondata.core.datastore.filesystem.AlluxioCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.HDFSCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.S3CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.ViewFSCarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileTypeInterface;

import org.apache.hadoop.conf.Configuration;

public class TestFileProvider implements FileTypeInterface {

  @Override public CarbonFile getCarbonFile(String path, Configuration configuration) {

    if (path.startsWith("testfs://")) {
      //Just sample for translation. Make it as local file path
      path = path.replace("testfs://", "/");
    }
    FileFactory.FileType fileType = FileFactory.getFileType(path);
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
      case HDFS:
        return new HDFSCarbonFile(path, configuration);
      case S3:
        return new S3CarbonFile(path, configuration);
      case ALLUXIO:
        return new AlluxioCarbonFile(path);
      case VIEWFS:
        return new ViewFSCarbonFile(path);
      default:
        return new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
    }
  }

  @Override public boolean isPathSupported(String path) {
    return path.startsWith("testfs://");
  }
}