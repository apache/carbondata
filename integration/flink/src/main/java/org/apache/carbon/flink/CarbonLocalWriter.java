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

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.carbon.core.metadata.SegmentManager;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

final class CarbonLocalWriter extends CarbonWriter {

  private static final Logger LOGGER
          = LogServiceFactory.getLogService(CarbonLocalWriter.class.getName());

  CarbonLocalWriter(
      final CarbonLocalWriterFactory factory,
      final CarbonTable table,
      final org.apache.carbondata.sdk.file.CarbonWriter writer,
      final String writePath,
      final String writePartition
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
    this.flushed = true;
  }

  private final CarbonLocalWriterFactory factory;

  private final CarbonTable table;

  private final org.apache.carbondata.sdk.file.CarbonWriter writer;

  private final String writePath;

  private final String writePartition;

  private volatile boolean flushed;

  @Override
  public CarbonLocalWriterFactory getFactory() {
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
    try {
      final Properties writerProperties = this.factory.getConfiguration().getWriterProperties();
      String dataPath = writerProperties.getProperty(CarbonLocalProperty.DATA_PATH);
      if (dataPath == null) {
        throw new IllegalArgumentException(
                "Writer property [" + CarbonLocalProperty.DATA_PATH + "] is not set."
        );
      }
      dataPath = dataPath + this.table.getDatabaseName() + "/"
          + this.table.getTableName() + "/" + this.writePartition + "/";
      this.uploadSegmentDataFiles(this.writePath + "Fact/Part0/Segment_null/", dataPath);
      try {
        this.writeSegmentFile(this.table, dataPath);
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

  private void uploadSegmentDataFiles(final String localPath, final String remotePath)
          throws IOException {
    final File[] files = new File(localPath).listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] start.");
      }
      try {
        final File remoteFile = new File(remotePath + file.getName());
        if (!remoteFile.exists()) {
          tryCreateLocalFile(remoteFile);
        }
        CarbonUtil.copyCarbonDataFileToCarbonStorePath(file.getAbsolutePath(), remotePath, 1024);
      } catch (CarbonDataWriterException exception) {
        LOGGER.error(exception.getMessage(), exception);
        throw exception;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] end.");
      }
    }
  }

  private void deleteSegmentDataFilesQuietly(final String segmentDataPath) {
    try {
      CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(segmentDataPath));
    } catch (Throwable exception) {
      LOGGER.error("Fail to delete segment data path [" + segmentDataPath + "].", exception);
    }
  }

  private void writeSegmentFile(final CarbonTable table, final String segmentLocation)
          throws IOException {
    final Map<String, String> options = new HashMap<>(2);
    options.put("path", segmentLocation);
    options.put("format", "carbon");
    LOGGER.info("Add segment[" + segmentLocation + "] to table[" + table.getTableName() + "].");
    tryCreateLocalDirectory(
        new File(CarbonTablePath.getLoadDetailsDir(
                table.getAbsoluteTableIdentifier().getTablePath()))
    );
    final CarbonFile segmentStatusFile = SegmentManager.createSegmentFile(table, options);
    try {
      this.writeSegmentSuccessFile(segmentStatusFile);
    } catch (Throwable exception) {
      SegmentManager.deleteSegmentFileQuietly(segmentStatusFile);
      throw exception;
    }
  }

  private void writeSegmentSuccessFile(final CarbonFile segmentStatusFile) throws IOException {
    final String segmentStatusSuccessFilePath = segmentStatusFile.getCanonicalPath() + ".success";
    final DataOutputStream segmentStatusSuccessOutputStream =
        FileFactory.getDataOutputStream(
            segmentStatusSuccessFilePath,
            FileFactory.getFileType(segmentStatusSuccessFilePath),
            CarbonCommonConstants.BYTEBUFFER_SIZE,
            1024
        );
    try {
      IOUtils.copyBytes(
          new ByteArrayInputStream(new byte[0]),
          segmentStatusSuccessOutputStream,
          CarbonCommonConstants.BYTEBUFFER_SIZE
      );
      segmentStatusSuccessOutputStream.flush();
    } finally {
      try {
        CarbonUtil.closeStream(segmentStatusSuccessOutputStream);
      } catch (IOException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
    }
  }

  private static void tryCreateLocalFile(final File file) throws IOException {
    if (file.exists()) {
      return;
    }
    if (file.getParentFile() != null) {
      tryCreateLocalDirectory(file.getParentFile());
    }
    if (!file.createNewFile()) {
      throw new IOException("File [" + file.getCanonicalPath() + "] is exist.");
    }
  }

  private static void tryCreateLocalDirectory(final File file) throws IOException {
    if (file.exists()) {
      return;
    }
    if (file.getParentFile() != null) {
      tryCreateLocalDirectory(file.getParentFile());
    }
    if (!file.mkdir() && LOGGER.isDebugEnabled()) {
      LOGGER.debug("Directory [" + file.getCanonicalPath() + "] is exist.");
    }
  }

}
