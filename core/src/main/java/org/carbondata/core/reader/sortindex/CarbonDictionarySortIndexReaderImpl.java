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
package org.carbondata.core.reader.sortindex;

import java.io.IOException;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.ColumnIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.reader.ThriftReader;
import org.carbondata.format.ColumnSortInfo;

import org.apache.thrift.TBase;

/**
 * Implementation for reading the dictionary sort index and inverted sort index .
 */
public class CarbonDictionarySortIndexReaderImpl implements CarbonDictionarySortIndexReader {

  /**
   * carbonTable Identifier holding the info of databaseName and tableName
   */
  private CarbonTableIdentifier carbonTableIdentifier;

  /**
   * column name
   */
  protected ColumnIdentifier columnIdentifier;

  /**
   * hdfs store location
   */
  private String carbonStorePath;

  /**
   * the path of the dictionary Sort Index file
   */
  private String sortIndexFilePath;

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
      LOGGER.error(ie,
          "problem while reading the column sort info.");
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
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTableIdentifier);
    this.sortIndexFilePath = carbonTablePath.getSortIndexFilePath(columnIdentifier.getColumnId());
    openThriftReader();
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
