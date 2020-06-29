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
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.StageInput;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;

import org.apache.carbon.core.metadata.StageManager;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

final class CarbonLocalWriter extends CarbonWriter {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonLocalWriter.class.getName());

  CarbonLocalWriter(
      final CarbonLocalWriterFactory factory,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3640
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
      final String identifier,
      final CarbonTable table,
      final String writePath
  ) {
    super(factory, identifier, table);
    final Properties writerProperties = factory.getConfiguration().getWriterProperties();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3655
    final Properties carbonProperties = factory.getConfiguration().getCarbonProperties();
    final String commitThreshold =
        writerProperties.getProperty(CarbonLocalProperty.COMMIT_THRESHOLD);
    this.writerFactory = new WriterFactory(table, writePath) {
      @Override
      protected org.apache.carbondata.sdk.file.CarbonWriter newWriter(
          final Object[] row) {
        try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3655
          final CarbonWriterBuilder writerBuilder =
              org.apache.carbondata.sdk.file.CarbonWriter.builder()
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3661
              .taskNo(UUID.randomUUID().toString().replace("-", ""))
              .outputPath(super.getWritePath(row))
              .writtenBy("flink")
              .withSchemaFile(CarbonTablePath.getSchemaFilePath(table.getTablePath()))
              .withCsvInput();
          for (String propertyName : carbonProperties.stringPropertyNames()) {
            try {
              writerBuilder.withLoadOption(propertyName,
                  carbonProperties.getProperty(propertyName));
            } catch (IllegalArgumentException exception) {
              LOGGER.warn("Fail to set load option [" + propertyName + "], may be unsupported.",
                  exception);
            }
          }
          return writerBuilder.build();
        } catch (IOException | InvalidLoadOptionException exception) {
          // TODO
          throw new UnsupportedOperationException(exception);
        }
      }
    };
    this.writePath = writePath;
    this.writeCommitThreshold =
        commitThreshold == null ? Long.MAX_VALUE : Long.parseLong(commitThreshold);
    this.writeCount = new AtomicLong(0);
    this.flushed = true;
  }

  private final WriterFactory writerFactory;

  private final String writePath;

  private final long writeCommitThreshold;

  private final AtomicLong writeCount;

  private volatile boolean flushed;

  @Override
  public String getPath() {
    return this.writePath;
  }

  @Override
  public void addElement(final Object[] element) throws IOException {
    this.writerFactory.getWriter(element).write(element);
    this.writeCount.incrementAndGet();
    if (this.writeCount.get() >= this.writeCommitThreshold) {
      this.closeWriters();
      this.commit();
      this.writerFactory.reset();
      this.writeCount.set(0);
    }
    this.flushed = false;
  }

  @Override
  public void flush() throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Flush writer. " + this.toString());
    }
    synchronized (this) {
      if (!this.flushed) {
        this.closeWriters();
        this.commit();
        this.writerFactory.reset();
        this.writeCount.set(0);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3676
      String dataPath = CarbonTablePath.getStageDataDir(this.table.getTablePath());
      tryCreateLocalDirectory(new File(dataPath));
      StageInput stageInput = this.uploadSegmentDataFiles(this.writePath, dataPath);
      if (stageInput == null) {
        return;
      }
      try {
        // make it ordered by time in case the files ordered by file name.
        String stageInputPath = CarbonTablePath.getStageDir(
            table.getAbsoluteTableIdentifier().getTablePath()) +
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3678
            CarbonCommonConstants.FILE_SEPARATOR + System.currentTimeMillis() + UUID.randomUUID();
        tryCreateLocalDirectory(new File(stageInputPath));
        StageManager.writeStageInput(stageInputPath, stageInput);
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3640
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
    if (this.writerFactory == null) {
      return;
    }
    try {
      synchronized (this) {
        if (!this.flushed) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3640
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
          this.closeWriters();
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

  private void closeWriters() throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3640
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3557
    if (this.writerFactory == null) {
      return;
    }
    final List<org.apache.carbondata.sdk.file.CarbonWriter> writers =
        this.writerFactory.getWriters();
    for (org.apache.carbondata.sdk.file.CarbonWriter writer : writers) {
      writer.close();
    }
  }

  private void deleteSegmentDataFilesQuietly(final String segmentDataPath) {
    try {
      CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(segmentDataPath));
    } catch (Throwable exception) {
      LOGGER.error("Fail to delete segment data path [" + segmentDataPath + "].", exception);
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
