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

package org.apache.carbondata.core.reader;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;

/**
 * This class perform the functionality of reading the delete delta file
 */
public class CarbonDeleteDeltaFileReaderImpl implements CarbonDeleteDeltaFileReader {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDeleteDeltaFileReaderImpl.class.getName());

  private String filePath;

  private FileFactory.FileType fileType;

  private DataInputStream dataInputStream = null;

  private InputStreamReader inputStream = null;

  private static final int DEFAULT_BUFFER_SIZE = 258;

  /**
   * @param filePath
   * @param fileType
   */
  public CarbonDeleteDeltaFileReaderImpl(String filePath, FileFactory.FileType fileType) {
    this.filePath = filePath;

    this.fileType = fileType;
  }

  /**
   * This method will be used to read complete delete delta file.
   * scenario:
   * Whenever a query is executed then read the delete delta file
   * to exclude the deleted data.
   *
   * @return All deleted records for the specified block
   * @throws IOException if an I/O error occurs
   */
  @Override public String read() throws IOException {
    // Configure Buffer based on our requirement
    char[] buffer = new char[DEFAULT_BUFFER_SIZE];
    StringWriter sw = new StringWriter();
    dataInputStream = FileFactory.getDataInputStream(filePath, fileType);
    inputStream = new InputStreamReader(dataInputStream,
        CarbonCommonConstants.DEFAULT_CHARSET);
    int n = 0;
    while (-1 != (n = inputStream.read(buffer))) {
      sw.write(buffer, 0, n);
    }
    return sw.toString();
  }

  /**
   * Reads delete delta file (json file) and returns DeleteDeltaBlockDetails
   * @return DeleteDeltaBlockDetails
   * @throws IOException
   */
  @Override public DeleteDeltaBlockDetails readJson() throws IOException {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    DeleteDeltaBlockDetails deleteDeltaBlockDetails;
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(filePath, FileFactory.getFileType(filePath));

    try {
      if (!FileFactory.isFileExist(filePath, FileFactory.getFileType(filePath))) {
        return new DeleteDeltaBlockDetails("");
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          CarbonCommonConstants.DEFAULT_CHARSET);
      buffReader = new BufferedReader(inStream);
      deleteDeltaBlockDetails =
          gsonObjectToRead.fromJson(buffReader, DeleteDeltaBlockDetails.class);
    } catch (IOException e) {
      return new DeleteDeltaBlockDetails("");
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    return deleteDeltaBlockDetails;
  }
}
