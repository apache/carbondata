/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.presto.processor.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.collector.impl.AbstractScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.FILE_SEPARATOR;
import static org.apache.carbondata.presto.constants.PrestoCommonConstants.COLUMNAR_DATA_READ_BATCH_SIZE;

import org.apache.commons.lang3.ArrayUtils;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class ColumnBasedResultCollector extends AbstractScannedResultCollector {

    protected QueryDimension[] queryDimensions;

    protected QueryMeasure[] queryMeasures;
    /**
     * query order
     */
    protected int[] order;
    private DirectDictionaryGenerator[] directDictionaryGenerators;
    private int[] actualIndexInSurrogateKey;

    private boolean[] dictionaryEncodingArray;

    private boolean[] directDictionaryEncodingArray;

    private boolean[] implictColumnArray;

    private boolean[] complexDataTypeArray;

    private int dictionaryColumnIndex;
    private int noDictionaryColumnIndex;
    private int complexTypeColumnIndex;

    private boolean isDimensionExists;

    private Map<Integer, GenericQueryType> comlexDimensionInfoMap;

    public ColumnBasedResultCollector(BlockExecutionInfo blockExecutionInfos) {
        super(blockExecutionInfos);
        queryDimensions = tableBlockExecutionInfos.getQueryDimensions();
        queryMeasures = tableBlockExecutionInfos.getQueryMeasures();
        initDimensionAndMeasureIndexesForFillingData();
        isDimensionExists = queryDimensions.length > 0;
        this.comlexDimensionInfoMap = tableBlockExecutionInfos.getComlexDimensionInfoMap();
    }

    /**
     * This method will add a record both key and value to list object
     * it will keep track of how many record is processed, to handle limit scenario
     */
    @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {

        batchSize = Integer.parseInt(COLUMNAR_DATA_READ_BATCH_SIZE);

        int noOfColumns = queryDimensions.length + queryMeasures.length;

        int resultSize = scannedResult.numberOfOutputRows();
        if (resultSize < batchSize) {
            batchSize = resultSize;
        }

        Object[][] matrix = new Object[noOfColumns][batchSize];

        // scan the record and add to list
        int rowCounter = 0;
        int[] surrogateResult;
        byte[][] noDictionaryKeys;
        byte[][] complexTypeKeyArray;
        while (scannedResult.hasNext() && rowCounter < batchSize) {
            Object[] row = new Object[queryDimensions.length + queryMeasures.length];
            if (isDimensionExists) {
                surrogateResult = scannedResult.getDictionaryKeyIntegerArray();
                noDictionaryKeys = scannedResult.getNoDictionaryKeyArray();
                complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
                dictionaryColumnIndex = 0;
                noDictionaryColumnIndex = 0;
                complexTypeColumnIndex = 0;
                for (int i = 0; i < queryDimensions.length; i++) {
                    fillDimensionData(scannedResult, surrogateResult, noDictionaryKeys, complexTypeKeyArray,
                            comlexDimensionInfoMap, row, i);
                }
            } else {
                scannedResult.incrementCounter();
            }
            if (scannedResult.containsDeletedRow(scannedResult.getCurrentRowId())) {
                continue;
            }
            fillMeasureData(scannedResult, row);

            for (int i = 0; i < noOfColumns; i++) {
                matrix[i][rowCounter] = row[i];
            }
            rowCounter++;
        }

        ArrayList<Object[]> columnarData;
        if (rowCounter < batchSize) {
            Object[][] matrix_temp = new Object[noOfColumns][rowCounter];
            for (int i = 0; i < noOfColumns; i++) {
                System.arraycopy(matrix[i], 0, matrix_temp[i], 0, rowCounter);
            }
            columnarData = new ArrayList<>(Arrays.asList(matrix_temp));
        } else {
            columnarData = new ArrayList<>(Arrays.asList(matrix));
        }

        if (noOfColumns == 0) {
            matrix = new Object[1][rowCounter];
            columnarData = new ArrayList<>(Arrays.asList(matrix));
        }

        return columnarData;
    }

    private void fillDimensionData(AbstractScannedResult scannedResult, int[] surrogateResult,
                                   byte[][] noDictionaryKeys, byte[][] complexTypeKeyArray,
                                   Map<Integer, GenericQueryType> comlexDimensionInfoMap, Object[] row, int i) {
        if (!dictionaryEncodingArray[i]) {
            if (implictColumnArray[i]) {
                if (CARBON_IMPLICIT_COLUMN_TUPLEID.equals(queryDimensions[i].getDimension().getColName())) {
                    row[order[i]] = DataTypeUtil.getDataBasedOnDataType(
                            scannedResult.getBlockletId() + FILE_SEPARATOR + scannedResult.getCurrentPageCounter()
                                    + FILE_SEPARATOR + scannedResult.getCurrentRowId(), DataType.STRING);
                } else {
                    row[order[i]] =
                            DataTypeUtil.getDataBasedOnDataType(scannedResult.getBlockletId(), DataType.STRING);
                }
            } else {
                row[order[i]] = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(
                        noDictionaryKeys[noDictionaryColumnIndex++],
                        queryDimensions[i].getDimension().getDataType());
            }
        } else if (directDictionaryEncodingArray[i]) {
            if (directDictionaryGenerators[i] != null) {
                row[order[i]] = directDictionaryGenerators[i].getValueFromSurrogate(
                        surrogateResult[actualIndexInSurrogateKey[dictionaryColumnIndex++]]);
            }
        } else if (complexDataTypeArray[i]) {
            row[order[i]] = comlexDimensionInfoMap.get(queryDimensions[i].getDimension().getOrdinal())
                    .getDataBasedOnDataTypeFromSurrogates(
                            ByteBuffer.wrap(complexTypeKeyArray[complexTypeColumnIndex++]));
            dictionaryColumnIndex++;
        } else {
            row[order[i]] = surrogateResult[actualIndexInSurrogateKey[dictionaryColumnIndex++]];
        }
    }

    private void fillMeasureData(AbstractScannedResult scannedResult, Object[] row) {
        if (measureInfo.getMeasureDataTypes().length > 0) {
            Object[] msrValues = new Object[measureInfo.getMeasureDataTypes().length];
            fillMeasureData(msrValues, 0, scannedResult);
            for (int i = 0; i < msrValues.length; i++) {
                row[order[i + queryDimensions.length]] = msrValues[i];
            }
        }
    }

    private void initDimensionAndMeasureIndexesForFillingData() {
        List<Integer> dictionaryIndexes = new ArrayList<Integer>();
        for (QueryDimension queryDimension : queryDimensions) {
            if (queryDimension.getDimension().hasEncoding(Encoding.DICTIONARY) || queryDimension
                    .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                dictionaryIndexes.add(queryDimension.getDimension().getOrdinal());
            }
        }
        int[] primitive =
                ArrayUtils.toPrimitive(dictionaryIndexes.toArray(new Integer[dictionaryIndexes.size()]));
        Arrays.sort(primitive);
        actualIndexInSurrogateKey = new int[dictionaryIndexes.size()];
        int index = 0;
        for (QueryDimension queryDimension : queryDimensions) {
            if (queryDimension.getDimension().hasEncoding(Encoding.DICTIONARY) || queryDimension
                    .getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
                actualIndexInSurrogateKey[index++] =
                        Arrays.binarySearch(primitive, queryDimension.getDimension().getOrdinal());
            }
        }

        dictionaryEncodingArray = CarbonUtil.getDictionaryEncodingArray(queryDimensions);
        directDictionaryEncodingArray = CarbonUtil.getDirectDictionaryEncodingArray(queryDimensions);
        implictColumnArray = CarbonUtil.getImplicitColumnArray(queryDimensions);
        complexDataTypeArray = CarbonUtil.getComplexDataTypeArray(queryDimensions);
        order = new int[queryDimensions.length + queryMeasures.length];
        for (int i = 0; i < queryDimensions.length; i++) {
            order[i] = queryDimensions[i].getQueryOrder();
        }
        for (int i = 0; i < queryMeasures.length; i++) {
            order[i + queryDimensions.length] = queryMeasures[i].getQueryOrder();
        }
        directDictionaryGenerators = new DirectDictionaryGenerator[queryDimensions.length];
        for (int i = 0; i < queryDimensions.length; i++) {
            directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
                    .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
        }
    }

}