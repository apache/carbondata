/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.reader.sortindex;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.factory.CarbonCommonFactory;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReader;
import org.apache.carbondata.core.reader.CarbonDictionaryMetadataReaderImpl;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.service.PathService;
import org.apache.carbondata.format.ColumnSortInfo;

import org.apache.thrift.TBase;

/**
 * Implementation for reading the dictionary sort index and inverted sort index .
 */
public class CarbonDictionarySortIndexReaderImpl implements CarbonDictionarySortIndexReader {

  /**
   * carbonTable Identifier holding the info of databaseName and tableName
   */
  protected CarbonTableIdentifier carbonTableIdentifier;

  /**
   * column name
   */
  protected ColumnIdentifier columnIdentifier;

  /**
   * store location
   */
  protected String carbonStorePath;

  /**
   * the path of the dictionary Sort Index file
   */
  protected String sortIndexFilePath;

  /**
   * Column sort info thrift instance.
   */
  ColumnSortInfo columnSortInfo = null;

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDictionarySortIndexReaderImpl.class.getName());

  /**
   * dictionary sortIndex file Reader
   */
  private ThriftReader dictionarySortIndexThriftReader;

  /**
   * @param carbonTableIdentifier Carbon Table identifier holding the database name and table name
   * @param columnIdentifier      column name
   * @param carbonStorePath       carbon store path
   */
  public CarbonDictionarySortIndexReaderImpl(final CarbonTableIdentifier carbonTableIdentifier,
      final ColumnIdentifier columnIdentifier, final String carbonStorePath) {
    this.carbonTableIdentifier = carbonTableIdentifier;
    this.columnIdentifier = columnIdentifier;
    this.carbonStorePath = carbonStorePath;
  }

  /**
   * method for reading the carbon dictionary sort index data
   * from columns sortIndex file.
   *
   * @return The method return's the list of dictionary sort Index and sort Index reverse
   * In case of no member for column empty list will be return
   * @throws IOException In case any I/O error occurs
   */
  @Override public List<Integer> readSortIndex() throws IOException {
    if (null == columnSortInfo) {
      readColumnSortInfo();
    }
    return columnSortInfo.getSort_index();
  }

  /**
   * method for reading the carbon dictionary sort index data
   * from columns sortIndex file.
   * In case of no member empty list will be return
   *
   * @throws IOException In case any I/O error occurs
   */
  private void readColumnSortInfo() throws IOException {
    init();
    try {
      columnSortInfo = (ColumnSortInfo) dictionarySortIndexThriftReader.read();
    } catch (IOException ie) {
      LOGGER.error(ie, "problem while reading the column sort info.");
      throw new IOException("problem while reading the column sort info.", ie);
    } finally {
      if (null != dictionarySortIndexThriftReader) {
        dictionarySortIndexThriftReader.close();
      }
    }
  }

  /**
   * method for reading the carbon dictionary inverted sort index data
   * from columns sortIndex file.
   *
   * @return The method return's the list of dictionary inverted sort Index
   * @throws IOException In case any I/O error occurs
   */
  @Override public List<Integer> readInvertedSortIndex() throws IOException {
    if (null == columnSortInfo) {
      readColumnSortInfo();
    }
    return columnSortInfo.getSort_index_inverted();
  }

  /**
   * The method initializes the dictionary Sort Index file path
   * and initialize and opens the thrift reader for dictionary sortIndex file.
   *
   * @throws IOException if any I/O errors occurs
   */
  private void init() throws IOException {
    initPath();
    openThriftReader();
  }

  protected void initPath() {
    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath =
        pathService.getCarbonTablePath(carbonStorePath, carbonTableIdentifier);
    try {
      CarbonDictionaryColumnMetaChunk chunkMetaObjectForLastSegmentEntry =
          getChunkMetaObjectForLastSegmentEntry();
      long dictOffset = chunkMetaObjectForLastSegmentEntry.getEnd_offset();
      this.sortIndexFilePath =
          carbonTablePath.getSortIndexFilePath(columnIdentifier.getColumnId(), dictOffset);
      if (!FileFactory
          .isFileExist(this.sortIndexFilePath, FileFactory.getFileType(this.sortIndexFilePath))) {
        this.sortIndexFilePath =
            carbonTablePath.getSortIndexFilePath(columnIdentifier.getColumnId());
      }
    } catch (IOException e) {
      this.sortIndexFilePath = carbonTablePath.getSortIndexFilePath(columnIdentifier.getColumnId());
    }

  }

  /**
   * This method will read the dictionary chunk metadata thrift object for last entry
   *
   * @return last entry of dictionary meta chunk
   * @throws IOException if an I/O error occurs
   */
  private CarbonDictionaryColumnMetaChunk getChunkMetaObjectForLastSegmentEntry()
      throws IOException {
    CarbonDictionaryMetadataReader columnMetadataReaderImpl = getDictionaryMetadataReader();
    try {
      // read the last segment entry for dictionary metadata
      return columnMetadataReaderImpl.readLastEntryOfDictionaryMetaChunk();
    } finally {
      // Close metadata reader
      columnMetadataReaderImpl.close();
    }
  }

  /**
   * @return
   */
  protected CarbonDictionaryMetadataReader getDictionaryMetadataReader() {
    return new CarbonDictionaryMetadataReaderImpl(carbonStorePath, carbonTableIdentifier,
        columnIdentifier);
  }

  /**
   * This method will open the dictionary sort index file stream for reading
   *
   * @throws IOException in case any I/O errors occurs
   */
  private void openThriftReader() throws IOException {
    this.dictionarySortIndexThriftReader =
        new ThriftReader(this.sortIndexFilePath, new ThriftReader.TBaseCreator() {
          @Override public TBase create() {
            return new ColumnSortInfo();
          }
        });
    dictionarySortIndexThriftReader.open();
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override public void close() throws IOException {
    if (null != dictionarySortIndexThriftReader) {
      dictionarySortIndexThriftReader.close();
    }
  }
}
