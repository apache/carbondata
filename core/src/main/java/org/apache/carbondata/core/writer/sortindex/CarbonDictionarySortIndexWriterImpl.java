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
package org.apache.carbondata.core.writer.sortindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.service.CarbonCommonFactory;
import org.apache.carbondata.core.service.PathService;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.format.ColumnSortInfo;

/**
 * The class responsible for writing the dictionary/column sort index and sort index inverted data
 * in the thrift format
 */
public class CarbonDictionarySortIndexWriterImpl implements CarbonDictionarySortIndexWriter {

  /**
   * carbonTable Identifier holding the info of databaseName and tableName
   */
  protected CarbonTableIdentifier carbonTableIdentifier;

  /**
   * column name
   */
  protected DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier;

  /**
   * carbon store location
   */
  protected String carbonStorePath;
  /**
   * Path of dictionary sort index file for which the sortIndex to be written
   */
  protected String sortIndexFilePath;
  /**
   * Instance of thrift writer to write the data
   */
  private ThriftWriter sortIndexThriftWriter;

  /**
   * Column sort info thrift instance.
   */
  private ColumnSortInfo columnSortInfo = new ColumnSortInfo();

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDictionarySortIndexWriterImpl.class.getName());

  /**
   * @param carbonStorePath       Carbon store path
   * @param carbonTableIdentifier table identifier which will give table name and database name
   * @param dictionaryColumnUniqueIdentifier      column unique identifier
   */
  public CarbonDictionarySortIndexWriterImpl(final CarbonTableIdentifier carbonTableIdentifier,
      final DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier,
      final String carbonStorePath) {
    this.carbonTableIdentifier = carbonTableIdentifier;
    this.dictionaryColumnUniqueIdentifier = dictionaryColumnUniqueIdentifier;
    this.carbonStorePath = carbonStorePath;
  }

  /**
   * The method is used populate the dictionary sortIndex data to columnSortInfo
   * in thrif format.
   *
   * @param sortIndexList list of sortIndex
   * @throws IOException In Case of any I/O errors occurs.
   */
  @Override public void writeSortIndex(List<Integer> sortIndexList) {
    columnSortInfo.setSort_index(sortIndexList);
  }

  /**
   * The method is used populate the dictionary Inverted sortIndex data to columnSortInfo
   * in thrif format.
   *
   * @param invertedSortIndexList list of  sortIndexInverted
   * @throws IOException In Case of any I/O errors occurs.
   */
  @Override public void writeInvertedSortIndex(List<Integer> invertedSortIndexList) {
    columnSortInfo.setSort_index_inverted(invertedSortIndexList);
  }

  /**
   * Initialize the sortIndexFilePath and open writing stream
   * for dictionary sortIndex file thrif writer
   * write the column sort info to the store when both sort index  and sort index
   * inverted are populated.
   * existing sort index file has to be overwritten with new sort index data
   * columnSortInfo having null sortIndex and invertedSortIndex will not be written
   */
  private void writeColumnSortInfo() throws IOException {
    boolean isNotNull =
        null != columnSortInfo.getSort_index() && null != columnSortInfo.sort_index_inverted;
    if (isNotNull) {
      initPath();
      String folderContainingFile = CarbonTablePath.getFolderContainingFile(this.sortIndexFilePath);
      boolean created = CarbonUtil.checkAndCreateFolder(folderContainingFile);
      if (!created) {
        LOGGER.error("Database metadata folder creation status :: " + created);
        throw new IOException("Failed to created database metadata folder");
      }
      try {

        this.sortIndexThriftWriter = new ThriftWriter(this.sortIndexFilePath, false);
        this.sortIndexThriftWriter.open();
        sortIndexThriftWriter.write(columnSortInfo);
      } catch (IOException ie) {
        LOGGER.error(ie,
            "problem while writing the dictionary sort index file.");
        throw new IOException("problem while writing the dictionary sort index file.", ie);
      } finally {
        if (null != sortIndexThriftWriter) {
          this.sortIndexThriftWriter.close();
        }
        this.sortIndexFilePath = null;
      }
    }
  }

  protected void initPath() {
    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath = pathService
        .getCarbonTablePath(carbonStorePath, carbonTableIdentifier,
            dictionaryColumnUniqueIdentifier);
    String dictionaryPath = carbonTablePath.getDictionaryFilePath(
        dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId());
    long dictOffset = CarbonUtil.getFileSize(dictionaryPath);
    this.sortIndexFilePath = carbonTablePath
        .getSortIndexFilePath(dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId(),
            dictOffset);
    cleanUpOldSortIndex(carbonTablePath, dictionaryPath);
  }

  /**
   * It cleans up old unused sortindex file
   *
   * @param carbonTablePath
   */
  protected void cleanUpOldSortIndex(CarbonTablePath carbonTablePath, String dictPath) {
    CarbonFile dictFile =
        FileFactory.getCarbonFile(dictPath, FileFactory.getFileType(dictPath));
    CarbonFile[] files = carbonTablePath.getSortIndexFiles(dictFile.getParentFile(),
        dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId());
    int maxTime;
    try {
      maxTime = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME));
    } catch (NumberFormatException e) {
      maxTime = CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
    }
    if (null != files) {
      Arrays.sort(files, new Comparator<CarbonFile>() {
        @Override public int compare(CarbonFile o1, CarbonFile o2) {
          return o1.getName().compareTo(o2.getName());
        }
      });
      for (int i = 0; i < files.length - 1; i++) {
        long difference = System.currentTimeMillis() - files[i].getLastModifiedTime();
        long minutesElapsed = (difference / (1000 * 60));
        if (minutesElapsed > maxTime) {
          if (!files[i].delete()) {
            LOGGER.warn("Failed to delete sortindex file." + files[i].getAbsolutePath());
          } else {
            LOGGER.info("Sort index file is deleted." + files[i].getAbsolutePath());
          }
        }
      }
    }
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override public void close() throws IOException {
    writeColumnSortInfo();
    if (null != sortIndexThriftWriter) {
      sortIndexThriftWriter.close();
    }
  }
}
