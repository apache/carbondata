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

package org.carbondata.processing.merger.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonSliceAndFiles;
import org.carbondata.processing.factreader.FactReaderInfo;
import org.carbondata.processing.factreader.CarbonSurrogateTupleHolder;
import org.carbondata.processing.merger.columnar.iterator.CarbonDataIterator;
import org.carbondata.processing.merger.columnar.iterator.impl.CarbonColumnarLeafTupleDataIterator;
import org.carbondata.processing.merger.columnar.iterator.impl.CarbonLeafTupleWrapperIterator;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.schema.metadata.CarbonColumnarFactMergerInfo;
import org.carbondata.processing.store.CarbonFactDataHandlerColumnarMerger;
import org.carbondata.processing.store.CarbonFactHandler;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;

public abstract class ColumnarFactFileMerger {

    /**
     * dataHandler
     */
    public CarbonFactHandler dataHandler;
    /**
     * otherMeasureIndex
     */
    protected int[] otherMeasureIndex;
    /**
     * customMeasureIndex
     */
    protected int[] customMeasureIndex;
    /**
     * mdkeyLength
     */
    protected int mdkeyLength;

    protected List<CarbonDataIterator<CarbonSurrogateTupleHolder>> leafTupleIteratorList;

    public ColumnarFactFileMerger(CarbonColumnarFactMergerInfo carbonColumnarFactMergerInfo,
            int currentRestructNumber) {
        this.mdkeyLength = carbonColumnarFactMergerInfo.getMdkeyLength();
        List<Integer> otherMeasureIndexList =
                new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<Integer> customMeasureIndexList =
                new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < carbonColumnarFactMergerInfo.getType().length; i++) {
            if (carbonColumnarFactMergerInfo.getType()[i] != 'c') {
                otherMeasureIndexList.add(i);
            } else {
                customMeasureIndexList.add(i);
            }
        }
        otherMeasureIndex = new int[otherMeasureIndexList.size()];
        customMeasureIndex = new int[customMeasureIndexList.size()];
        for (int i = 0; i < otherMeasureIndex.length; i++) {
            otherMeasureIndex[i] = otherMeasureIndexList.get(i);
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            customMeasureIndex[i] = customMeasureIndexList.get(i);
        }

        this.leafTupleIteratorList = new ArrayList<CarbonDataIterator<CarbonSurrogateTupleHolder>>(
                carbonColumnarFactMergerInfo.getSlicesFromHDFS().size());
        CarbonDataIterator<CarbonSurrogateTupleHolder> leaftTupleIterator = null;
        for (CarbonSliceAndFiles sliceInfo : carbonColumnarFactMergerInfo.getSlicesFromHDFS()) {

            leaftTupleIterator = new CarbonLeafTupleWrapperIterator(sliceInfo.getKeyGen(),
                    carbonColumnarFactMergerInfo.getGlobalKeyGen(),
                    new CarbonColumnarLeafTupleDataIterator(sliceInfo.getPath(),
                            sliceInfo.getSliceFactFilesList(),
                            getFactReaderInfo(carbonColumnarFactMergerInfo), mdkeyLength));
            if (leaftTupleIterator.hasNext()) {
                leaftTupleIterator.fetchNextData();
                leafTupleIteratorList.add(leaftTupleIterator);
            }
        }
        dataHandler = new CarbonFactDataHandlerColumnarMerger(carbonColumnarFactMergerInfo,
                currentRestructNumber);
    }

    public abstract void mergerSlice() throws SliceMergerException;

    private FactReaderInfo getFactReaderInfo(
            CarbonColumnarFactMergerInfo carbonColumnarFactMergerInfo) {
        FactReaderInfo factReaderInfo = new FactReaderInfo();
        String[] aggType = new String[carbonColumnarFactMergerInfo.getMeasureCount()];

        Arrays.fill(aggType, "n");
        if (null != carbonColumnarFactMergerInfo.getAggregators()) {
            for (int i = 0; i < aggType.length; i++) {
                if (carbonColumnarFactMergerInfo.getAggregators()[i]
                        .equals(CarbonCommonConstants.CUSTOM) || carbonColumnarFactMergerInfo
                        .getAggregators()[i].equals(CarbonCommonConstants.DISTINCT_COUNT)) {
                    aggType[i] = "c";
                } else {
                    aggType[i] = "n";
                }
            }
        }
        factReaderInfo.setCubeName(carbonColumnarFactMergerInfo.getCubeName());
        factReaderInfo.setSchemaName(carbonColumnarFactMergerInfo.getSchemaName());
        factReaderInfo.setMeasureCount(carbonColumnarFactMergerInfo.getMeasureCount());
        factReaderInfo.setTableName(carbonColumnarFactMergerInfo.getTableName());
        factReaderInfo.setDimLens(carbonColumnarFactMergerInfo.getDimLens());
        int[] blockIndex = new int[carbonColumnarFactMergerInfo.getDimLens().length];
        for (int i = 0; i < blockIndex.length; i++) {
            blockIndex[i] = i;
        }
        factReaderInfo.setBlockIndex(blockIndex);
        factReaderInfo.setUpdateMeasureRequired(true);

        return factReaderInfo;
    }

    /**
     * Below method will be used to add sorted row
     *
     * @throws SliceMergerException
     */
    protected void addRow(CarbonSurrogateTupleHolder molapTuple) throws SliceMergerException {
        Object[] row = new Object[molapTuple.getMeasures().length + 1];
        System.arraycopy(molapTuple.getMeasures(), 0, row, 0, molapTuple.getMeasures().length);
        row[row.length - 1] = molapTuple.getMdKey();
        try {
            this.dataHandler.addDataToStore(row);
        } catch (CarbonDataWriterException e) {
            throw new SliceMergerException("Problem in merging the slice", e);
        }
    }

}
