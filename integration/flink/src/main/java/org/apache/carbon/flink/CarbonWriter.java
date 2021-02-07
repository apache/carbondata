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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.StageInput;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.log4j.Logger;

/**
 * This class is a wrapper of CarbonWriter in SDK.
 * It not only write data to carbon with CarbonWriter in SDK, also generate segment file.
 */
public abstract class CarbonWriter extends ProxyFileWriter<Object[]> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonWriter.class.getName());

  public CarbonWriter(final CarbonWriterFactory factory,
      final String identifier, final CarbonTable table) {
    ProxyFileWriterFactory.register(factory.getType(), factory.getClass());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Open writer. " + this.toString());
    }
    this.factory = factory;
    this.identifier = identifier;
    this.table = table;
  }

  private final CarbonWriterFactory factory;

  private final String identifier;

  protected final CarbonTable table;

  @Override
  public CarbonWriterFactory getFactory() {
    return this.factory;
  }

  @Override
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * @return when there is no data file uploaded, then return <code>null</code>.
   */
  protected StageInput uploadSegmentDataFiles(final String localPath, final String remotePath)
      throws IOException {
    if (!this.table.isHivePartitionTable()) {
      final CarbonFile[] files = FileFactory.getCarbonFile(localPath).listFiles();
      if (files == null || files.length == 0) {
        return null;
      }
      Map<String, Long> fileNameMapLength = new HashMap<>(files.length);
      for (CarbonFile file : files) {
        fileNameMapLength.put(file.getName(), file.getLength());
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] start.");
        }
        try {
          CarbonUtil.copyCarbonDataFileToCarbonStorePath(file.getAbsolutePath(), remotePath,
                  1024 * 1024 * 2);
        } catch (CarbonDataWriterException exception) {
          LOGGER.error(exception.getMessage(), exception);
          throw exception;
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] end.");
        }
      }
      return new StageInput(remotePath, fileNameMapLength);
    } else {
      final List<StageInput.PartitionLocation> partitionLocationList = new ArrayList<>();
      final List<String> partitions = new ArrayList<>();
      uploadSegmentDataFiles(FileFactory.getCarbonFile(localPath), remotePath,
          partitionLocationList, partitions);
      if (partitionLocationList.isEmpty()) {
        return null;
      } else {
        return new StageInput(remotePath, partitionLocationList);
      }
    }
  }

  private static void uploadSegmentDataFiles(
      final CarbonFile directory, final String remotePath,
      final List<StageInput.PartitionLocation> partitionLocationList,
      final List<String> partitions
  ) throws IOException {
    final CarbonFile[] files = directory.listFiles();
    if (files == null || files.length == 0) {
      return;
    }
    Map<String, Long> fileNameMapLength = new HashMap<>();
    for (CarbonFile file : files) {
      if (file.isDirectory()) {
        partitions.add(file.getName());
        uploadSegmentDataFiles(file, remotePath, partitionLocationList, partitions);
        partitions.remove(partitions.size() - 1);
        continue;
      }
      fileNameMapLength.put(file.getName(), file.getLength());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] start.");
      }
      try {
        CarbonUtil.copyCarbonDataFileToCarbonStorePath(file.getAbsolutePath(), remotePath,
                  1024 * 1024 * 2);
      } catch (CarbonDataWriterException exception) {
        LOGGER.error(exception.getMessage(), exception);
        throw exception;
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upload file[" + file.getAbsolutePath() + "] to [" + remotePath + "] end.");
      }
    }
    if (!fileNameMapLength.isEmpty()) {
      final Map<String, String> partitionMap = new HashMap<>(partitions.size());
      for (String partition : partitions) {
        final String[] segments = partition.split("=");
        partitionMap.put(segments[0].trim(), segments[1].trim());
      }
      partitionLocationList.add(
          new StageInput.PartitionLocation(
              partitionMap,
              fileNameMapLength
          )
      );
    }
  }

  protected abstract static class WriterFactory {

    public WriterFactory(final CarbonTable table, final String writePath) {
      final List<ColumnSchema> partitionColumns;
      if (table.getPartitionInfo() == null) {
        partitionColumns = Collections.emptyList();
      } else {
        partitionColumns = table.getPartitionInfo().getColumnSchemaList();
      }
      this.table = table;
      this.partitionColumns = partitionColumns;
      this.writePath = writePath;
      this.root = new Node();
      this.writers = new ArrayList<>();
    }

    private final CarbonTable table;

    private final List<ColumnSchema> partitionColumns;

    private final String writePath;

    private final Node root;

    private final List<org.apache.carbondata.sdk.file.CarbonWriter> writers;

    public List<org.apache.carbondata.sdk.file.CarbonWriter> getWriters() {
      return this.writers;
    }

    public org.apache.carbondata.sdk.file.CarbonWriter getWriter(final Object[] row) {
      Node node = this.root;
      for (int index = 0; index < this.partitionColumns.size(); index++) {
        final Object columnValue = row[this.partitionColumns.get(index).getSchemaOrdinal()];
        if (columnValue == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        Node child = node.children.get(columnValue);
        if (child == null) {
          child = new Node();
          node.children.put(columnValue, child);
        }
        node = child;
      }
      if (node.writer == null) {
        node.writer = this.newWriter(row);
        this.writers.add(node.writer);
      }
      return node.writer;
    }

    protected String getWritePath(final Object[] row) {
      if (this.partitionColumns.isEmpty()) {
        return this.writePath;
      }
      final StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(this.writePath);
      for (int index = 0; index < this.partitionColumns.size(); index++) {
        final ColumnSchema columnSchema = this.partitionColumns.get(index);
        final Object columnValue = row[columnSchema.getSchemaOrdinal()];
        stringBuilder.append(columnSchema.getColumnName());
        stringBuilder.append("=");
        stringBuilder.append(columnValue.toString());
        stringBuilder.append(CarbonCommonConstants.FILE_SEPARATOR);
      }
      return stringBuilder.toString();
    }

    protected abstract org.apache.carbondata.sdk.file.CarbonWriter newWriter(final Object[] row);

    public void reset() {
      this.writers.clear();
      this.root.children.clear();
      this.root.writer = null;
    }

    private static final class Node {

      final Map<Object, Node> children = new HashMap<>();

      org.apache.carbondata.sdk.file.CarbonWriter writer;

    }

  }

}
