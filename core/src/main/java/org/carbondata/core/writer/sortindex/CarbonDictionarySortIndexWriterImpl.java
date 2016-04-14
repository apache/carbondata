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
package org.carbondata.core.writer.sortindex;

import java.io.IOException;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.path.CarbonSharedDictionaryPath;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.ThriftWriter;
import org.carbondata.format.ColumnSortInfo;

/**
 * The class responsible for writing the dictionary/column sort index and sort index inverted data
 * in the thrift format
 */
public class CarbonDictionarySortIndexWriterImpl implements CarbonDictionarySortIndexWriter {

    /**
     * carbonTable Identifier holding the info of databaseName and tableName
     */
    private CarbonTableIdentifier carbonTableIdentifier;

    /**
     * column name
     */
    private String columnIdentifier;

    /**
     * carbon store location
     */
    private String carbonStorePath;
    /**
     * Path of dictionary sort index file for which the sortIndex to be written
     */
    private String sortIndexFilePath;
    /**
     * Instance of thrift writer to write the data
     */
    private ThriftWriter sortIndexThriftWriter;

    /**
     * dimension type identifier <boolean> shared/private to table
     */
    private boolean isSharedDimension;

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
     * @param columnIdentifier      column unique identifier
     * @param isSharedDimension     flag for shared dimension
     */
    public CarbonDictionarySortIndexWriterImpl(final CarbonTableIdentifier carbonTableIdentifier,
            final String columnIdentifier, final String carbonStorePath,
            final boolean isSharedDimension) {
        this.carbonTableIdentifier = carbonTableIdentifier;
        this.columnIdentifier = columnIdentifier;
        this.carbonStorePath = carbonStorePath;
        this.isSharedDimension = isSharedDimension;
    }

    /**
     * The method is used populate the dictionary sortIndex data to columnSortInfo
     * in thrif format.
     *
     * @param sortIndexList list of sortIndex
     * @throws IOException In Case of any I/O errors occurs.
     */
    @Override public void writeSortIndex(List<Integer> sortIndexList) throws IOException {
        columnSortInfo.setSort_index(sortIndexList);
    }

    /**
     * The method is used populate the dictionary Inverted sortIndex data to columnSortInfo
     *  in thrif format.
     *
     * @param invertedSortIndexList list of  sortIndexInverted
     * @throws IOException In Case of any I/O errors occurs.
     */
    @Override public void writeInvertedSortIndex(List<Integer> invertedSortIndexList)
            throws IOException {
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
        boolean isNotNull = null != columnSortInfo.getSort_index()
                && null != columnSortInfo.sort_index_inverted;
        if (isNotNull) {
            if (isSharedDimension) {
                this.sortIndexFilePath = CarbonSharedDictionaryPath
                        .getSortIndexFilePath(carbonStorePath,
                                carbonTableIdentifier.getDatabaseName(), columnIdentifier);
            } else {
                CarbonTablePath carbonTablePath =
                        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTableIdentifier);
                this.sortIndexFilePath = carbonTablePath.getSortIndexFilePath(columnIdentifier);
            }
            String folderContainingFile =
                    CarbonTablePath.getFolderContainingFile(this.sortIndexFilePath);
            boolean created = CarbonUtil.checkAndCreateFolder(folderContainingFile);
            if (!created) {
                LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                        "Database metadata folder creation status :: " + created);
                throw new IOException("Failed to created database metadata folder");
            }
            try {

                this.sortIndexThriftWriter = new ThriftWriter(this.sortIndexFilePath, false);
                this.sortIndexThriftWriter.open();
                sortIndexThriftWriter.write(columnSortInfo);
            } catch (IOException ie) {
                LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, ie,
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
