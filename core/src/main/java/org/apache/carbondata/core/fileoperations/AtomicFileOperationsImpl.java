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

package org.apache.carbondata.core.fileoperations;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.conf.Configuration;

public class AtomicFileOperationsImpl implements AtomicFileOperations {

  private String filePath;

  private FileType fileType;

  private String tempWriteFilePath;

  private DataOutputStream dataOutStream;

  private Configuration configuration;

  public AtomicFileOperationsImpl(Configuration configuration, String filePath,
      FileType fileType) {
    this.configuration = configuration;
    this.filePath = filePath;
    this.fileType = fileType;
  }

  @Override public DataInputStream openForRead() throws IOException {
    return FileFactory.getDataInputStream(configuration, filePath, fileType);
  }

  @Override public DataOutputStream openForWrite(FileWriteOperation operation) throws IOException {

    filePath = filePath.replace("\\", "/");

    tempWriteFilePath = filePath + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;

    if (FileFactory.isFileExist(configuration, tempWriteFilePath, fileType)) {
      FileFactory.getCarbonFile(configuration, tempWriteFilePath, fileType).delete();
    }

    FileFactory.createNewFile(configuration, tempWriteFilePath, fileType);

    dataOutStream = FileFactory.getDataOutputStream(configuration, tempWriteFilePath, fileType);

    return dataOutStream;

  }

  @Override public void close() throws IOException {

    if (null != dataOutStream) {
      CarbonUtil.closeStream(dataOutStream);
      CarbonFile tempFile = FileFactory.getCarbonFile(configuration, tempWriteFilePath, fileType);
      if (!tempFile.renameForce(filePath)) {
        throw new IOException("temporary file renaming failed, src="
            + tempFile.getPath() + ", dest=" + filePath);
      }
    }

  }

}
