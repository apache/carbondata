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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * class that implements cacheable interface and methods specific to column dictionary
 */
public abstract class AbstractColumnDictionaryInfo implements DictionaryInfo {

    /**
     * list that will hold all the dictionary chunks for one column
     */
    protected List<List<byte[]>> dictionaryChunks =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * atomic integer to maintain the access count for a column access
     */
    protected AtomicInteger accessCount = new AtomicInteger();

    /**
     * file timestamp
     */
    protected long fileTimeStamp;

    /**
     * offset till where file is read
     */
    protected long offsetTillFileIsRead;

    /**
     * This method will return the timestamp of file based on which decision
     * the decision will be taken whether to read that file or not
     *
     * @return
     */
    @Override public long getFileTimeStamp() {
        return fileTimeStamp;
    }

    /**
     * This method will return the access count for a column based on which decision will be taken
     * whether to keep the object in memory
     *
     * @return
     */
    @Override public int getAccessCount() {
        return accessCount.get();
    }

    /**
     * This method will return the memory size of a column
     *
     * @return
     */
    @Override public long getMemorySize() {
        return offsetTillFileIsRead;
    }

    /**
     * This method will increment the access count for a column by 1
     * whenever a column is getting used in query or incremental data load
     */
    @Override public void incrementAccessCount() {
        accessCount.incrementAndGet();
    }

    /**
     * This method will decrement the access count for a column by 1
     * whenever a column usage is complete
     */
    private void decrementAccessCount() {
        if (accessCount.get() > 0) {
            accessCount.decrementAndGet();
        }
    }

    /**
     * This method will update the end offset of file everytime a file is read
     *
     * @param offsetTillFileIsRead
     */
    @Override public void setOffsetTillFileIsRead(long offsetTillFileIsRead) {
        this.offsetTillFileIsRead = offsetTillFileIsRead;
    }

    /**
     * This method will update the timestamp of a file if a file is modified
     * like in case of incremental load
     *
     * @param fileTimeStamp
     */
    @Override public void setFileTimeStamp(long fileTimeStamp) {
        this.fileTimeStamp = fileTimeStamp;
    }

    /**
     * This method will release the objects and set default value for primitive types
     */
    @Override public void clear() {
        decrementAccessCount();
    }

    /**
     * This method will find and return the dictionary value for a given surrogate key.
     * Applicable scenarios:
     * 1. Query final result preparation : While convert the final result which will
     * be surrogate key back to original dictionary values this method will be used
     *
     * @param surrogateKey a unique ID for a dictionary value
     * @return value if found else null
     */
    @Override public String getDictionaryValueForKey(int surrogateKey) {
        String dictionaryValue = null;
        int totalSizeOfDictionaryChunksTraversed = 0;
        for (List<byte[]> oneDictionaryChunk : dictionaryChunks) {
            totalSizeOfDictionaryChunksTraversed =
                    totalSizeOfDictionaryChunksTraversed + oneDictionaryChunk.size();
            // skip the dictionary chunk till surrogate key is lesser than size of
            // dictionary chunks traversed
            if (totalSizeOfDictionaryChunksTraversed < surrogateKey) {
                continue;
            }
            // lets say surrogateKey = 26, total size traversed is 28, dictionary chunk size = 12
            // then surrogate position in dictionary chunk list is = 26 - (28-12) - 1 = 9
            // -1 because list index starts from 0
            int surrogatePositionInDictionaryChunk =
                    surrogateKey - (totalSizeOfDictionaryChunksTraversed - oneDictionaryChunk
                            .size()) - 1;
            byte[] dictionaryValueInBytes =
                    oneDictionaryChunk.get(surrogatePositionInDictionaryChunk);
            dictionaryValue = new String(dictionaryValueInBytes,
                    Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
            break;
        }
        return dictionaryValue;
    }
}
