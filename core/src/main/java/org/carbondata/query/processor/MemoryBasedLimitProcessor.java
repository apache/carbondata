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

package org.carbondata.query.processor;

import java.util.List;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.result.iterator.MemoryBasedResultIterator;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class MemoryBasedLimitProcessor implements DataProcessorExt {
    private int limit;

    private QueryResult result;

    public MemoryBasedLimitProcessor() {
        result = new QueryResult();
    }

    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException {
        limit = model.getLimit();
    }

    @Override
    public void finish() throws DataProcessorException {

    }

    @Override
    public CarbonIterator<QueryResult> getQueryResultIterator() {
        return new MemoryBasedResultIterator(result);
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException {

        if (limit == -1 || result.size() < limit) {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key);
            result.add(arrayWrapper, value);
        }

    }

    /**
     * While processing the row the direct surrogate key values will be added directly as byte[] to
     * ByteArrayWrapper instance, Internally list will be maintaned in each ByteArrayWrapper instance
     * inorder to hold the direct surrogate key value for different surrogate keys.
     */
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value)
            throws DataProcessorException {
        if (limit == -1 || result.size() < limit) {
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper.setMaskedKey(key.getMaskedKey());
            List<byte[]> listOfDirectKey = key.getNoDictionaryValKeyList();
            if (null != listOfDirectKey) {
                for (byte[] byteArray : listOfDirectKey) {
                    arrayWrapper.addToNoDictionaryValKeyList(byteArray);
                }
            }
            List<byte[]> listOfComplexTypes = key.getCompleteComplexTypeData();
            if (null != listOfComplexTypes) {
                for (byte[] byteArray : listOfComplexTypes) {
                    arrayWrapper.addComplexTypeData(byteArray);
                }
            }
            result.add(arrayWrapper, value);
        }

    }

}
