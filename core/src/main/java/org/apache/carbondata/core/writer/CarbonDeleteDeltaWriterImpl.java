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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * This class is responsible for writing the delete delta file
 */
public class CarbonDeleteDeltaWriterImpl implements CarbonDeleteDeltaWriter {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonDeleteDeltaWriterImpl.class.getName());

  private String filePath;

  private DataOutputStream dataOutStream = null;

  /**
   * @param filePath
   */
  public CarbonDeleteDeltaWriterImpl(String filePath) {
    this.filePath = filePath;

  }

  /**
   * This method will write the deleted records data in the json format.
   * @param deleteDeltaBlockDetails
   * @throws IOException
   */
  @Override
  public void write(DeleteDeltaBlockDetails deleteDeltaBlockDetails) throws IOException {
    BufferedWriter brWriter = null;
    try {
      FileFactory.createNewFile(filePath);
      dataOutStream = FileFactory.getDataOutputStream(filePath);
      Gson gsonObjectToWrite = new Gson();
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutStream,
          CarbonCommonConstants.DEFAULT_CHARSET));
      String deletedData = gsonObjectToWrite.toJson(deleteDeltaBlockDetails);
      brWriter.write(deletedData);
    } catch (IOException ioe) {
      LOGGER.error("Error message: " + ioe.getLocalizedMessage());
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      if (null != dataOutStream) {
        dataOutStream.flush();
      }
      if (null != brWriter) {
        brWriter.close();
      }
    }

  }
}
