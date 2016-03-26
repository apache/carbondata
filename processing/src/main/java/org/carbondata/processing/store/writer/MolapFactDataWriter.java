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

package org.carbondata.processing.store.writer;

import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.processing.store.writer.exception.MolapDataWriterException;

public interface MolapFactDataWriter<T> {

    /**
     * This method will be used to write leaf data to file
     * file format
     * <key><measure1><measure2>....
     *
     * @param dataArray  measure array
     * @param entryCount number of entries
     * @param startKey   start key of leaf
     * @param endKey     end key of leaf
     * @throws MolapDataWriterException throws new MolapDataWriterException if any problem
     */

    void writeDataToFile(IndexStorage<T>[] keyStorageArray, byte[][] dataArray, int entryCount,
            byte[] startKey, byte[] endKey) throws MolapDataWriterException;

    /**
     * Below method will be used to write the leaf meta data to file
     *
     * @throws MolapDataWriterException
     */
    void writeleafMetaDataToFile() throws MolapDataWriterException;

    /**
     * Below method will be used to initialise the writer
     */
    void initializeWriter() throws MolapDataWriterException;

    /**
     * Below method will be used to close the writer
     */
    void closeWriter();

    /**
     * Below method will be used to get the leaf meta data size
     */
    int getLeafMetadataSize();

    /**
     * For getting TempLocation
     *
     * @return
     */
    String getTempStoreLocation();

    /**
     * @param isNoDictionary
     */
    void setIsNoDictionary(boolean[] isNoDictionary);

}
