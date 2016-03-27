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

package org.carbondata.processing.factreader.columnar;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.processing.factreader.FactReaderInfo;
import org.carbondata.processing.factreader.CarbonSurrogateTupleHolder;
import org.carbondata.processing.iterator.CarbonIterator;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;

public class CarbonColumnarLeafTupleIterator implements CarbonIterator<CarbonSurrogateTupleHolder> {

    /**
     * unique value if slice
     */
    private Object[] uniqueValue;

    /**
     * hash next
     */
    private boolean hasNext;

    /**
     * leaf node iterator
     */
    private CarbonIterator<AbstractColumnarScanResult> leafNodeIterator;

    /**
     * measureCount
     */
    private int measureCount;

    /**
     * aggType
     */
    private char[] aggType;

    /**
     * keyValue
     */
    private AbstractColumnarScanResult keyValue;

    private boolean isMeasureUpdateResuired;

    /**
     * rowCounter
     */

    /**
     * MolapSliceTupleIterator constructor to initialise
     *
     * @param mdkeyLength mdkey length
     */
    public CarbonColumnarLeafTupleIterator(String sliceLocation, CarbonFile[] factFiles,
            FactReaderInfo factItreatorInfo, int mdkeyLength) {
        this.measureCount = factItreatorInfo.getMeasureCount();
        ValueCompressionModel compressionModel =
                getCompressionModel(sliceLocation, factItreatorInfo.getTableName(), measureCount);
        this.uniqueValue = compressionModel.getUniqueValue();
        this.leafNodeIterator =
                new CarbonColumnarLeafNodeIterator(factFiles, mdkeyLength, compressionModel,
                        factItreatorInfo);
        this.aggType = compressionModel.getType();
        initialise();
        this.isMeasureUpdateResuired = factItreatorInfo.isUpdateMeasureRequired();
    }

    public CarbonColumnarLeafTupleIterator(String loadPath, CarbonFile[] factFiles,
            FactReaderInfo factReaderInfo, int mdKeySize, LeafNodeInfoColumnar leafNodeInfoColumnar) {
        this.measureCount = factReaderInfo.getMeasureCount();
        ValueCompressionModel compressionModel =
                getCompressionModel(loadPath, factReaderInfo.getTableName(), measureCount);
        this.uniqueValue = compressionModel.getUniqueValue();
        this.leafNodeIterator =
                new CarbonColumnarLeafNodeIterator(factFiles, mdKeySize, compressionModel,
                        factReaderInfo, leafNodeInfoColumnar);
        this.aggType = compressionModel.getType();
        initialise();
        this.isMeasureUpdateResuired = factReaderInfo.isUpdateMeasureRequired();

    }

    /**
     * below method will be used to initialise
     */
    private void initialise() {
        if (this.leafNodeIterator.hasNext()) {
            keyValue = leafNodeIterator.next();
            this.hasNext = true;
        }
    }

    /**
     * This method will be used to get the compression model for slice
     *
     * @param measureCount measure count
     * @return compression model
     */
    private ValueCompressionModel getCompressionModel(String sliceLocation, String tableName,
            int measureCount) {
        ValueCompressionModel compressionModel = ValueCompressionUtil.getValueCompressionModel(
                sliceLocation + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
                        + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT, measureCount);
        return compressionModel;
    }

    /**
     * below method will be used to get the measure value from measure data
     * wrapper
     *
     * @return
     */
    private Object[] getMeasure() {
        Object[] measures = new Object[measureCount];
        Object values = 0;
        for (int i = 0; i < measures.length; i++) {
            if (aggType[i] == 'n') {
                values = keyValue.getDoubleValue(i);
                if (isMeasureUpdateResuired && !values.equals(uniqueValue[i])) {
                    measures[i] = values;
                }
            } else {
                measures[i] = keyValue.getByteArrayValue(i);
            }
        }
        return measures;
    }

    /**
     * below method will be used to check whether any data is present in the
     * slice
     */
    @Override
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * below method will be used to get the slice tuple
     */
    @Override
    public CarbonSurrogateTupleHolder next() {
        CarbonSurrogateTupleHolder tuple = new CarbonSurrogateTupleHolder();
        tuple.setSurrogateKey(keyValue.getKeyArray());
        tuple.setMeasures(getMeasure());
        if (keyValue.hasNext()) {
            return tuple;
        } else if (!leafNodeIterator.hasNext()) {
            hasNext = false;
        } else {
            initialise();
        }
        return tuple;
    }
}
