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

package org.carbondata.query.scanner.impl;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.filters.InMemoryFilter;
import org.carbondata.query.scanner.BTreeScanner;
import org.carbondata.query.util.MolapEngineLogEvent;

/**
 * This class will be used to scan data store based on filter present in the query
 * Class Description :
 * Version 1.0
 */
public class FilterTreeScanner extends BTreeScanner {
    /**
     *
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FilterTreeScanner.class.getName());
    /**
     *
     */
    protected InMemoryFilter filter;
    /**
     *
     */
    protected boolean filterPresent;
    private boolean smartJump;

    public FilterTreeScanner(byte[] startKey, byte[] endKey, KeyGenerator keyGenerator,
            KeyValue currKey, int[] msrs, FileHolder fileHolder, boolean smartJump) {
        super(startKey, endKey, keyGenerator, currKey, msrs, fileHolder);
        this.smartJump = smartJump;
    }

    /**
     * This method will check whether data is there to be read.
     *
     * @return true if the element is there to be read.
     */
    protected boolean hasNext() {
        boolean hasNext = false;
        if (block == null) {
            return false;
        }
        while (!hasNext) {
            if (currKey.isReset()) {
                currKey.setReset(false);
            } else if (index >= blockKeys) {
                block = block.getNext();
                index = 0;
                if (block == null) {
                    break;
                }
                blockKeys = block.getnKeys() - 1;
                currKey.setBlock(block, fileHolder);
                currKey.resetOffsets();
            } else {
                currKey.increment();
                index++;
            }

            // currKey = block.getNextKeyValue(index);
            // if(currKey == null)
            // {
            // break;
            // }

            if (endKey != null && keyGenerator
                    .compare(currKey.getArray(), currKey.getKeyOffset(), currKey.getKeyLength(),
                            endKey, 0, endKey.length) > 0) {
                // endKey will not be null in case filter is present or scanner
                // is run parallel for a slice
                break;
            }
            if (!filter.filterKey(currKey)) {
                if (smartJump) {
                    byte[] key = filter.getNextJump(currKey);
                    if (key != null) {
                        // Check style fix
                        if (!searchInternal(key)) {
                            return false;
                        }
                    } else if (index < blockKeys) {
                        index++;
                        currKey.increment();
                        currKey.setReset(true);
                    }
                }
                continue;
            }
            // got the desired element break now, but increment the index so
            // that scanner reads the next key when hasnext is called again;
            hasNext = true;
            break;
        }
        return hasNext;
    }

    /**
     * @param key
     * @return
     */
    private boolean searchInternal(byte[] key) {
        int pos = currKey.searchInternal(key, keyGenerator);
        if (pos < 0) {
            store.getNext(key, this);
            if (block == null) {
                // if the search for the jump key doesn't return any
                // block then there is no more parsing
                return false;
            }
        } else {
            index = pos;
            currKey.setRow(index);
            currKey.setReset(true);
        }
        return true;
    }

    /**
     * @param filter
     */
    public void setFilter(InMemoryFilter filter) {
        this.filter = filter;
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "BTreeScanner: filterPresent = " + filterPresent);
    }

}
