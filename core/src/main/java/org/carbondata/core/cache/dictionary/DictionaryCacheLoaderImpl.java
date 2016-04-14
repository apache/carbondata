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

package org.carbondata.core.cache.dictionary;

import java.io.IOException;
import java.util.List;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.reader.CarbonDictionaryReader;
import org.carbondata.core.reader.CarbonDictionaryReaderImpl;

/**
 * This class is responsible for loading the dictionary data for given columns
 */
public class DictionaryCacheLoaderImpl implements DictionaryCacheLoader {

    /**
     * carbon table identifier
     */
    private CarbonTableIdentifier carbonTableIdentifier;

    /**
     * carbon store path
     */
    private String carbonStorePath;

    /**
     * @param carbonTableIdentifier fully qualified table name
     * @param carbonStorePath       hdfs store path
     */
    public DictionaryCacheLoaderImpl(CarbonTableIdentifier carbonTableIdentifier,
            String carbonStorePath) {
        this.carbonTableIdentifier = carbonTableIdentifier;
        this.carbonStorePath = carbonStorePath;
    }

    /**
     * This method will load the dictionary data for a given columnIdentifier
     *
     * @param dictionaryInfo             dictionary info object which will hold the required data
     *                                   for a given column
     * @param columnIdentifier           column unique identifier
     * @param dictionaryChunkStartOffset start offset from where dictionary file has to
     *                                   be read
     * @param dictionaryChunkEndOffset   end offset till where dictionary file has to
     *                                   be read
     * @throws IOException
     */
    @Override public void load(DictionaryInfo dictionaryInfo, String columnIdentifier,
            long dictionaryChunkStartOffset, long dictionaryChunkEndOffset) throws IOException {
        List<byte[]> dictionaryChunk =
                load(columnIdentifier, dictionaryChunkStartOffset, dictionaryChunkEndOffset);
        dictionaryInfo.addDictionaryChunk(dictionaryChunk);
    }

    /**
     * This method will load the dictionary data between a given start and end offset
     *
     * @param columnIdentifier column unique identifier
     * @param startOffset      start offset of dictionary file
     * @param endOffset        end offset of dictionary file
     * @return list of dictionary value
     * @throws IOException
     */
    private List<byte[]> load(String columnIdentifier, long startOffset, long endOffset)
            throws IOException {
        CarbonDictionaryReader dictionaryReader = getDictionaryReader(columnIdentifier);
        List<byte[]> dictionaryValue = null;
        try {
            dictionaryValue = dictionaryReader.read(startOffset, endOffset);
        } finally {
            dictionaryReader.close();
        }
        return dictionaryValue;
    }

    /**
     * This method will create a dictionary reader instance to read the dictionary file
     *
     * @param columnIdentifier unique column identifier
     * @return carbon dictionary reader instance
     * @throws IOException
     */
    private CarbonDictionaryReader getDictionaryReader(String columnIdentifier) {
        CarbonDictionaryReader dictionaryReader =
                new CarbonDictionaryReaderImpl(carbonStorePath, carbonTableIdentifier,
                        columnIdentifier, false);
        return dictionaryReader;
    }
}
