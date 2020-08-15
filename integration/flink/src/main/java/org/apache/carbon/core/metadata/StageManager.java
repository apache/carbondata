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

package org.apache.carbon.core.metadata;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.statusmanager.StageInput;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public final class StageManager {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(StageManager.class.getName());

  public static void writeStageInput(final String stageInputPath, final StageInput stageInput)
          throws IOException {
    AtomicFileOperations fileWrite =
        AtomicFileOperationFactory.getAtomicFileOperations(stageInputPath);
    BufferedWriter writer = null;
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      writer = new BufferedWriter(new OutputStreamWriter(dataOutputStream, StandardCharsets.UTF_8));
      String metadataInstance = new Gson().toJson(stageInput);
      writer.write(metadataInstance);
    } catch (IOException e) {
      LOGGER.error("Error message: " + e.getLocalizedMessage());
      fileWrite.setFailed();
      throw e;
    } finally {
      if (null != writer) {
        writer.flush();
      }
      CarbonUtil.closeStreams(writer);
      fileWrite.close();
    }

    try {
      writeSuccessFile(stageInputPath + CarbonTablePath.SUCCESS_FILE_SUFFIX);
    } catch (Throwable exception) {
      try {
        CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(stageInputPath));
      } catch (Throwable e) {
        LOGGER.error("Fail to delete stage input meta data [" + stageInputPath + "].", exception);
      }
      throw exception;
    }
  }

  private static void writeSuccessFile(final String successFilePath) throws IOException {
    final DataOutputStream segmentStatusSuccessOutputStream =
        FileFactory.getDataOutputStream(successFilePath,
            CarbonCommonConstants.BYTEBUFFER_SIZE, 1024 * 1024 * 2);
    try {
      IOUtils.copyBytes(
          new ByteArrayInputStream(new byte[0]),
          segmentStatusSuccessOutputStream,
          CarbonCommonConstants.BYTEBUFFER_SIZE);
      segmentStatusSuccessOutputStream.flush();
    } finally {
      try {
        CarbonUtil.closeStream(segmentStatusSuccessOutputStream);
      } catch (IOException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
    }
  }

}
