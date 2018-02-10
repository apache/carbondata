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

package org.apache.carbondata.core.writer;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;

/**
 * This class is responsible for writing the delete delta file
 */
public class CarbonDeleteDeltaWriterImpl implements CarbonDeleteDeltaWriter {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDeleteDeltaWriterImpl.class.getName());

  private String filePath;

  private FileFactory.FileType fileType;

  private DataOutputStream dataOutStream = null;

  /**
   * @param filePath
   * @param fileType
   */
  public CarbonDeleteDeltaWriterImpl(String filePath, FileFactory.FileType fileType) {
    this.filePath = filePath;
    this.fileType = fileType;

  }

  /**
   * This method will write the deleted records data in to disk.
   *
   * @param value deleted records
   * @throws IOException if an I/O error occurs
   */
  @Override public void write(String value) throws IOException {
    BufferedWriter brWriter = null;
    try {
      FileFactory.createNewFile(filePath, fileType);
      dataOutStream = FileFactory.getDataOutputStream(filePath, fileType);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutStream,
          CarbonCommonConstants.DEFAULT_CHARSET));
      brWriter.write(value);
    } catch (IOException ioe) {
      LOGGER.error("Error message: " + ioe.getLocalizedMessage());
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      if (null != dataOutStream) {
        dataOutStream.flush();
      }
      CarbonUtil.closeStreams(brWriter, dataOutStream);
    }

  }

  /**
   * This method will write the deleted records data in the json format.
   * @param deleteDeltaBlockDetails
   * @throws IOException
   */
  @Override public void write(DeleteDeltaBlockDetails deleteDeltaBlockDetails) throws IOException {
    BufferedWriter brWriter = null;
    try {
      FileFactory.createNewFile(filePath, fileType);
      dataOutStream = FileFactory.getDataOutputStream(filePath, fileType);
      Gson gsonObjectToWrite = new Gson();
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutStream,
          CarbonCommonConstants.DEFAULT_CHARSET));
      String deletedData = gsonObjectToWrite.toJson(deleteDeltaBlockDetails);
      brWriter.write(deletedData);
    } catch (IOException ioe) {
      LOGGER.error("Error message: " + ioe.getLocalizedMessage());
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      if (null != dataOutStream) {
        dataOutStream.flush();
      }
      CarbonUtil.closeStreams(brWriter, dataOutStream);
    }

  }
}
