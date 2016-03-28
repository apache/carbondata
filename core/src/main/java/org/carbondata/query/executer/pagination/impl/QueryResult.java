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

package org.carbondata.query.executer.pagination.impl;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class QueryResult {
    private List<ByteArrayWrapper> keys;

    private List<MeasureAggregator[]> values;

    private int rsize;

    public QueryResult() {
        keys = new ArrayList<ByteArrayWrapper>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        values = new ArrayList<MeasureAggregator[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    }

    public void add(ByteArrayWrapper key, MeasureAggregator[] value) {
        keys.add(key);
        values.add(value);
        rsize++;
    }

    public int size() {
        return rsize;
    }

    public QueryResultIterator iterator() {
        return new QueryResultIterator();
    }

    public void prepareResult(KeyValueHolder[] data) {
        for (int i = 0; i < data.length; i++) {
            keys.add(data[i].key);
            values.add(data[i].value);
        }
        rsize = keys.size();
    }

    public class QueryResultIterator {
        private int counter = -1;

        public boolean hasNext() {
            ++counter;
            return counter < rsize;
        }

        public ByteArrayWrapper getKey() {
            return keys.get(counter);
        }

        public MeasureAggregator[] getValue() {
            return values.get(counter);
        }
    }

}
