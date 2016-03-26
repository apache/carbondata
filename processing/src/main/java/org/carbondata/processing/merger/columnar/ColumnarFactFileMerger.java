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

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.util.*;
import org.carbondata.processing.factreader.FactReaderInfo;
import org.carbondata.processing.factreader.MolapSurrogateTupleHolder;
import org.carbondata.processing.merger.columnar.iterator.MolapDataIterator;
import org.carbondata.processing.merger.columnar.iterator.impl.MolapColumnarLeafTupleDataIterator;
import org.carbondata.processing.merger.columnar.iterator.impl.MolapLeafTupleWrapperIterator;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.schema.metadata.MolapColumnarFactMergerInfo;
import org.carbondata.processing.store.MolapFactDataHandlerColumnarMerger;
import org.carbondata.processing.store.MolapFactHandler;
import org.carbondata.processing.store.writer.exception.MolapDataWriterException;

public abstract class ColumnarFactFileMerger {

    /**
     * dataHandler
     */
    public MolapFactHandler dataHandler;
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

    protected List<MolapDataIterator<MolapSurrogateTupleHolder>> leafTupleIteratorList;

    public ColumnarFactFileMerger(MolapColumnarFactMergerInfo molapColumnarFactMergerInfo,
            int currentRestructNumber) {
        this.mdkeyLength = molapColumnarFactMergerInfo.getMdkeyLength();
        List<Integer> otherMeasureIndexList =
                new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<Integer> customMeasureIndexList =
                new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < molapColumnarFactMergerInfo.getType().length; i++) {
            if (molapColumnarFactMergerInfo.getType()[i] != 'c') {
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

        this.leafTupleIteratorList = new ArrayList<MolapDataIterator<MolapSurrogateTupleHolder>>(
                molapColumnarFactMergerInfo.getSlicesFromHDFS().size());
        MolapDataIterator<MolapSurrogateTupleHolder> leaftTupleIterator = null;
        for (MolapSliceAndFiles sliceInfo : molapColumnarFactMergerInfo.getSlicesFromHDFS()) {

            leaftTupleIterator = new MolapLeafTupleWrapperIterator(sliceInfo.getKeyGen(),
                    molapColumnarFactMergerInfo.getGlobalKeyGen(),
                    new MolapColumnarLeafTupleDataIterator(sliceInfo.getPath(),
                            sliceInfo.getSliceFactFilesList(),
                            getFactReaderInfo(molapColumnarFactMergerInfo), mdkeyLength));
            if (leaftTupleIterator.hasNext()) {
                leaftTupleIterator.fetchNextData();
                leafTupleIteratorList.add(leaftTupleIterator);
            }
        }
        dataHandler = new MolapFactDataHandlerColumnarMerger(molapColumnarFactMergerInfo,
                currentRestructNumber);
    }

    public abstract void mergerSlice() throws SliceMergerException;

    private FactReaderInfo getFactReaderInfo(
            MolapColumnarFactMergerInfo molapColumnarFactMergerInfo) {
        FactReaderInfo factReaderInfo = new FactReaderInfo();
        String[] aggType = new String[molapColumnarFactMergerInfo.getMeasureCount()];

        Arrays.fill(aggType, "n");
        if (null != molapColumnarFactMergerInfo.getAggregators()) {
            for (int i = 0; i < aggType.length; i++) {
                if (molapColumnarFactMergerInfo.getAggregators()[i]
                        .equals(MolapCommonConstants.CUSTOM) || molapColumnarFactMergerInfo
                        .getAggregators()[i].equals(MolapCommonConstants.DISTINCT_COUNT)) {
                    aggType[i] = "c";
                } else {
                    aggType[i] = "n";
                }
            }
        }
        factReaderInfo.setCubeName(molapColumnarFactMergerInfo.getCubeName());
        factReaderInfo.setSchemaName(molapColumnarFactMergerInfo.getSchemaName());
        factReaderInfo.setMeasureCount(molapColumnarFactMergerInfo.getMeasureCount());
        factReaderInfo.setTableName(molapColumnarFactMergerInfo.getTableName());
        factReaderInfo.setDimLens(molapColumnarFactMergerInfo.getDimLens());
        int[] blockIndex = new int[molapColumnarFactMergerInfo.getDimLens().length];
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
    protected void addRow(MolapSurrogateTupleHolder molapTuple) throws SliceMergerException {
        Object[] row = new Object[molapTuple.getMeasures().length + 1];
        System.arraycopy(molapTuple.getMeasures(), 0, row, 0, molapTuple.getMeasures().length);
        row[row.length - 1] = molapTuple.getMdKey();
        try {
            this.dataHandler.addDataToStore(row);
        } catch (MolapDataWriterException e) {
            throw new SliceMergerException("Problem in merging the slice", e);
        }
    }

}
