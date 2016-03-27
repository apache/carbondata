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

package org.carbondata.processing.merger.columnar.iterator.impl;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.processing.factreader.CarbonSurrogateTupleHolder;
import org.carbondata.processing.merger.columnar.iterator.CarbonDataIterator;

/**
 * This class is a wrapper class over CarbonColumnarLeafTupleDataIterator.
 * This uses the global key gen for generating key.
 */
public class CarbonLeafTupleWrapperIterator implements
        CarbonDataIterator<CarbonSurrogateTupleHolder> {
    /**
     * logger
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonLeafTupleWrapperIterator.class.getName());
    CarbonDataIterator<CarbonSurrogateTupleHolder> iterator;
    private KeyGenerator localKeyGen;
    private KeyGenerator globalKeyGen;

    public CarbonLeafTupleWrapperIterator(KeyGenerator localKeyGen, KeyGenerator globalKeyGen,
            CarbonDataIterator<CarbonSurrogateTupleHolder> iterator) {
        this.iterator = iterator;
        this.localKeyGen = localKeyGen;
        this.globalKeyGen = globalKeyGen;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void fetchNextData() {
        iterator.fetchNextData();
    }

    @Override
    public CarbonSurrogateTupleHolder getNextData() {
        CarbonSurrogateTupleHolder nextData = iterator.getNextData();
        byte[] mdKey = nextData.getMdKey();
        long[] keyArray = localKeyGen.getKeyArray(mdKey);
        byte[] generateKey = null;
        try {
            generateKey = globalKeyGen.generateKey(keyArray);
        } catch (KeyGenException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Error occurred :: " + e.getMessage());
        }
        nextData.setSurrogateKey(generateKey);
        return nextData;
    }
}
