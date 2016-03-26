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

package org.carbondata.query.result.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.result.Result;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class MapBasedResult implements Result<Map<ByteArrayWrapper, MeasureAggregator[]>, Void> {
    private Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> resultIterator;

    private Entry<ByteArrayWrapper, MeasureAggregator[]> tuple;

    private Map<ByteArrayWrapper, MeasureAggregator[]> scannerResult;

    private int resulSize;

    public MapBasedResult() {
        scannerResult = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        this.resultIterator = scannerResult.entrySet().iterator();
    }

    @Override public ByteArrayWrapper getKey() {
        tuple = this.resultIterator.next();
        return tuple.getKey();
    }

    @Override public MeasureAggregator[] getValue() {
        return tuple.getValue();
    }

    @Override public boolean hasNext() {
        return this.resultIterator.hasNext();
    }

    @Override
    public void addScannedResult(Map<ByteArrayWrapper, MeasureAggregator[]> scannerResult, Void v) {
        this.scannerResult = scannerResult;
        resulSize = scannerResult.size();
        this.resultIterator = scannerResult.entrySet().iterator();
    }

    @Override public void merge(Result<Map<ByteArrayWrapper, MeasureAggregator[]>, Void> result) {
        ByteArrayWrapper key = null;
        MeasureAggregator[] value = null;
        Map<ByteArrayWrapper, MeasureAggregator[]> otherResult = result.getKeys();
        if (otherResult != null) {
            while (resultIterator.hasNext()) {
                Entry<ByteArrayWrapper, MeasureAggregator[]> entry = resultIterator.next();
                key = entry.getKey();
                value = entry.getValue();
                MeasureAggregator[] agg = otherResult.get(key);
                if (agg != null) {
                    for (int j = 0; j < agg.length; j++) {
                        agg[j].merge(value[j]);
                    }
                } else {
                    otherResult.put(key, value);
                }
            }
            resulSize = otherResult.size();
            this.resultIterator = otherResult.entrySet().iterator();
            this.scannerResult = otherResult;
        }
    }

    @Override public int size() {
        return resulSize;
    }

    @Override public Map<ByteArrayWrapper, MeasureAggregator[]> getKeys() {
        return this.scannerResult;
    }

    @Override public Void getValues() {
        return null;
    }
}
