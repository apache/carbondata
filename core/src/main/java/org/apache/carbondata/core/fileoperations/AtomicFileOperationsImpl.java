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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.log4j.Logger;

class AtomicFileOperationsImpl implements AtomicFileOperations {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2745

  /**
   * Logger instance
   */
  private static final Logger LOGGER =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2749
      LogServiceFactory.getLogService(AtomicFileOperationsImpl.class.getName());
  private String filePath;

  private String tempWriteFilePath;

  private DataOutputStream dataOutStream;
  private boolean setFailed;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
  AtomicFileOperationsImpl(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public DataInputStream openForRead() throws IOException {
    return FileFactory.getDataInputStream(filePath);
  }

  @Override
  public DataOutputStream openForWrite(FileWriteOperation operation) throws IOException {

    filePath = filePath.replace("\\", "/");

    tempWriteFilePath = filePath + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
    if (FileFactory.isFileExist(tempWriteFilePath)) {
      FileFactory.getCarbonFile(tempWriteFilePath).delete();
    }

    FileFactory.createNewFile(tempWriteFilePath);

    dataOutStream = FileFactory.getDataOutputStream(tempWriteFilePath);

    return dataOutStream;

  }

  @Override
  public void close() throws IOException {
    if (null != dataOutStream) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1277
      CarbonUtil.closeStream(dataOutStream);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
      CarbonFile tempFile = FileFactory.getCarbonFile(tempWriteFilePath);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2749
      if (!this.setFailed) {
        if (!tempFile.renameForce(filePath)) {
          throw new IOException(
              "temporary file renaming failed, src=" + tempFile.getPath() + ", dest=" + filePath);
        }
      } else {
        LOGGER.warn("The temporary file renaming skipped due to I/O error, deleting file "
            + tempWriteFilePath);
        tempFile.delete();
      }
    }
  }

  @Override
  public void setFailed() {
    this.setFailed = true;
  }
}
