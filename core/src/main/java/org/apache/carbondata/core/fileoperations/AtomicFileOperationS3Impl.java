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

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * This Implementation for AtomicFileOperation is specific to S3 store.
 * In this temporary files would not be written instead directly overwrite call
 * would be fired on the desired file.
 *
 * This is required because deletion and recreation on tablestatus has a very small window where the
 * file would not exist in the Metadata directory. Any query which tries to access tablestatus
 * during this time will fail. By this fix the complete object will be overwritten to the bucket and
 * S3 will ensure that either the old or the new file content is always available for read.
 *
 */
class AtomicFileOperationS3Impl implements AtomicFileOperations {

  private String filePath;

  private DataOutputStream dataOutStream;

  AtomicFileOperationS3Impl(String filePath) {
    this.filePath = filePath;
  }
  @Override public DataInputStream openForRead() throws IOException {
    return FileFactory.getDataInputStream(filePath, FileFactory.getFileType(filePath));
  }

  @Override public void close() throws IOException {
    if (null != dataOutStream) {
      CarbonUtil.closeStream(dataOutStream);
    }
  }

  @Override public DataOutputStream openForWrite(FileWriteOperation operation) throws IOException {
    filePath = filePath.replace("\\", "/");
    FileFactory.FileType fileType = FileFactory.getFileType(filePath);
    dataOutStream = FileFactory.getDataOutputStream(filePath, fileType);
    return dataOutStream;
  }

  @Override public void setFailed() {
    // no implementation required
  }
}
