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
package org.apache.carbon.flink;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.carbon.core.metadata.StageManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.StageInput;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

final class CarbonS3Writer extends CarbonWriter {

  private static final Logger LOGGER
          = LogServiceFactory.getLogService(CarbonS3Writer.class.getName());

  CarbonS3Writer(
      final CarbonS3WriterFactory factory,
      final CarbonTable table,
      final org.apache.carbondata.sdk.file.CarbonWriter writer,
      final String writePath,
      final String writePartition,
      final Configuration configuration
  ) {
    ProxyFileWriterFactory.register(factory.getType(), factory.getClass());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Open writer. " + this.toString());
    }
    this.factory = factory;
    this.table = table;
    this.writer = writer;
    this.writePath = writePath;
    this.writePartition = writePartition;
    this.configuration = configuration;
    this.flushed = true;
  }

  private final CarbonS3WriterFactory factory;

  private final CarbonTable table;

  private final org.apache.carbondata.sdk.file.CarbonWriter writer;

  private final String writePath;

  private final String writePartition;

  private final Configuration configuration;

  private volatile boolean flushed;

  @Override
  public CarbonS3WriterFactory getFactory() {
    return this.factory;
  }

  @Override
  public String getPartition() {
    return this.writePartition;
  }

  @Override
  public void addElement(final String element) throws IOException {
    this.writer.write(element);
    this.flushed = false;
  }

  @Override
  public void flush() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Flush writer. " + this.toString());
    }
    synchronized (this) {
      if (!this.flushed) {
        this.writer.close();
        this.flushed = true;
      }
    }
  }

  @Override
  public void finish() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Finish writer. " + this.toString());
    }
    if (!this.flushed) {
      this.flush();
    }
  }

  @Override
  public void commit() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Commit write. " + this.toString());
    }
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(this.configuration);
    ThreadLocalSessionInfo.getOrCreateCarbonSessionInfo()
        .getNonSerializableExtraInfo().put("carbonConf", this.configuration);
    try {
      final Properties writerProperties = this.factory.getConfiguration().getWriterProperties();
      String dataPath = writerProperties.getProperty(CarbonS3Property.DATA_PATH);
      if (dataPath == null) {
        throw new IllegalArgumentException(
                "Writer property [" + CarbonS3Property.DATA_PATH + "] is not set."
        );
      }
      if (!dataPath.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
        throw new IllegalArgumentException(
                "Writer property [" + CarbonS3Property.DATA_PATH + "] is not a s3a path."
        );
      }
      dataPath = dataPath + this.table.getDatabaseName() + "/"
          + this.table.getTableName() + "/" + this.writePartition + "/";
      Map<String, Long> fileList =
              this.uploadSegmentDataFiles(this.writePath + "Fact/Part0/Segment_null/", dataPath);
      try {
        String stageInputPath = CarbonTablePath.getStageDir(table.getAbsoluteTableIdentifier().getTablePath()) +
                "/" + this.writePartition;
        StageManager.writeStageInput(stageInputPath, new StageInput(dataPath, fileList));
      } catch (Throwable exception) {
        this.deleteSegmentDataFilesQuietly(dataPath);
        throw exception;
      }
    } finally {
      try {
        FileUtils.deleteDirectory(new File(this.writePath));
      } catch (IOException exception) {
        LOGGER.error("Fail to delete write path [" + this.writePath + "].", exception);
      }
    }
  }

  @Override
  public void close() {
    if (this.writer == null) {
      return;
    }
    try {
      synchronized (this) {
        if (!this.flushed) {
          this.writer.close();
          this.flushed = true;
        }
      }
    } catch (Throwable exception) {
      LOGGER.error("Fail to close carbon writer.", exception);
    } finally {
      try {
        FileUtils.deleteDirectory(new File(this.writePath));
      } catch (IOException exception) {
        LOGGER.error("Fail to delete write path [" + this.writePath + "].", exception);
      }
    }
  }

  private Map<String, Long>  uploadSegmentDataFiles(final String localPath, final String remotePath) {
    final File[] files = new File(localPath).listFiles();
    if (files == null) {
      return new HashMap<>(0);
    }
    Map<String, Long> fileNameMapLength = new HashMap<>(files.length);
    for (File file : files) {
      fileNameMapLength.put(file.getName(), file.length());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] start.");
      }
      try {
        CarbonUtil.copyCarbonDataFileToCarbonStorePath(file.getAbsolutePath(), remotePath, 1024);
      } catch (CarbonDataWriterException exception) {
        LOGGER.error(exception.getMessage(), exception);
        throw exception;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] end.");
      }
    }
    return fileNameMapLength;
  }

  private void deleteSegmentDataFilesQuietly(final String segmentDataPath) {
    try {
      CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(segmentDataPath));
    } catch (Throwable exception) {
      LOGGER.error("Fail to delete segment data path [" + segmentDataPath + "].", exception);
    }
  }
}
