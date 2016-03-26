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

package org.carbondata.query.executer.normalize;

import java.util.*;
import java.util.Map.Entry;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.datastorage.InMemoryCube;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public final class NormalizeUtil {
    private NormalizeUtil() {

    }

    /**
     * Converts.
     *
     * @param data
     * @param normalizedStartingIndexArray
     * @param keyGenerator
     * @param normalizeKeyGen
     * @param maskedKeyRanges
     * @param slices
     * @param dims
     * @param replacedDims
     * @param listOfMapsOfDimValues
     * @param keySize
     * @param actualMaskByteRanges
     * @param maxKey
     * @return
     * @throws KeyGenException
     */
    public static Map<ByteArrayWrapper, MeasureAggregator[]> convert(
            Map<ByteArrayWrapper, MeasureAggregator[]> data,
            List<Integer> normalizedStartingIndexArray, KeyGenerator keyGenerator,
            KeyGenerator normalizeKeyGen, int[] maskedKeyRanges, List<InMemoryCube> slices,
            Dimension[] dims, Dimension[] replacedDims,
            List<Int2ObjectMap<List<int[]>>> listOfMapsOfDimValues, int keySize,
            int[] actualMaskByteRanges, byte[] maxKey) throws KeyGenException {
        Set<Entry<ByteArrayWrapper, MeasureAggregator[]>> entries = data.entrySet();
        int normalizedStartingIndexArraySize = normalizedStartingIndexArray.size();
        int dimensionCount = dims.length;
        Map<ByteArrayWrapper, MeasureAggregator[]> resultData =
                new HashMap<ByteArrayWrapper, MeasureAggregator[]>(
                        MolapCommonConstants.CONSTANT_SIZE_TEN);
        for (Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator =
             entries.iterator(); iterator.hasNext(); ) {
            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
            ByteArrayWrapper keyWrapper = entry.getKey();

            long[] keyArray = keyGenerator.getKeyArray(keyWrapper.getMaskedKey(), maskedKeyRanges);
            int[] value = new int[dimensionCount];
            for (int i = 0; i < dimensionCount; i++) {//CHECKSTYLE:OFF    Approval No:Approval-286
                value[i] = (int) keyArray[dims[i].getOrdinal()];
            }//CHECKSTYLE:ON

            //            MeasureAggregator[] d = entry.getValue();
            //            for(int i = 0;i < msrCount;i++)
            //            {
            //                if(d[i] instanceof AbstractMeasureAggregator)
            //                {
            //                    measureWithCustomAgg[i] = 1;
            //                    idVsValueMap.put(++valueIDCounter, d[i].getValueObject());
            //                    value[dimensionCount + i] = valueIDCounter;
            //                }
            //                else
            //                {
            //                    value[dimensionCount + i] = d[i].getValue();
            //                }
            //            }

            //int[] normalizedArray = new int[normalizedStartingIndexArray.size()];

            //replcing norm keys
            List<NormalizeRow> normList =
                    new ArrayList<NormalizeRow>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            normList.add(new NormalizeRow(value, entry.getValue()));
            List<NormalizeRow> updateNormalizedSurrogates =
                    updateNormalizedSurrogatesWithOriginal(listOfMapsOfDimValues,
                            normalizedStartingIndexArray, 0, normList);

            for (int i = 1; i < normalizedStartingIndexArraySize; i++) {
                updateNormalizedSurrogates =
                        updateNormalizedSurrogatesWithOriginal(listOfMapsOfDimValues,
                                normalizedStartingIndexArray, i, updateNormalizedSurrogates);

            }

            generateData(updateNormalizedSurrogates, replacedDims, normalizeKeyGen, keySize,
                    actualMaskByteRanges, maxKey, resultData, slices);
        }
        return resultData;

    }

    private static Map<ByteArrayWrapper, MeasureAggregator[]> generateData(
            List<NormalizeRow> normList, Dimension[] replacedDims, KeyGenerator normalizeKeyGen,
            int keySize, int[] actualMaskByteRanges, byte[] maxKey,
            Map<ByteArrayWrapper, MeasureAggregator[]> resultData, List<InMemoryCube> slices)
            throws KeyGenException {
        int dimCount = normalizeKeyGen.getDimCount();
        for (NormalizeRow row : normList) {
            int[] key = new int[dimCount];
            for (int i = 0; i < replacedDims.length; i++) {
                key[replacedDims[i].getOrdinal()] = row.key[i];
                key[replacedDims[i].getOrdinal()] =
                        getSortIndexById(replacedDims[i], key[replacedDims[i].getOrdinal()],
                                slices);
            }

            byte[] generateKey = normalizeKeyGen.generateKey(key);
            ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
            arrayWrapper
                    .setMaskedKey(getMaskedKey(generateKey, keySize, actualMaskByteRanges, maxKey));
            MeasureAggregator[] agg = resultData.put(arrayWrapper, row.value);
            if (agg != null) {
                for (int i = 0; i < agg.length; i++) {
                    row.value[i].merge(agg[i]);
                }
            }
        }
        return resultData;
    }

    /**
     * Below method will be used to get the sor index
     *
     * @param columnName
     * @param id
     * @return sort index
     */
    private static int getSortIndexById(Dimension columnName, int id, List<InMemoryCube> slices) {
        for (InMemoryCube slice : slices) {
            int index = slice.getMemberCache(
                    columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName
                            .getDimName() + '_' + columnName.getHierName()).getSortedIndex(id);
            if (index != -MolapCommonConstants.DIMENSION_DEFAULT) {
                return index;
            }
        }
        return -MolapCommonConstants.DIMENSION_DEFAULT;
    }

    /**
     * Below method will be used to get the masked key
     *
     * @param data
     * @return maskedKey
     */
    private static byte[] getMaskedKey(byte[] data, int keySize, int[] actualMaskByteRanges,
            byte[] maxKey) {
        byte[] maskedKey = new byte[keySize];
        int counter = 0;
        int byteRange = 0;
        for (int i = 0; i < keySize; i++) {
            byteRange = actualMaskByteRanges[i];
            maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
        }
        return maskedKey;
    }

    /**
     * @param listOfMapsOfDimValues
     * @param normalizedStartingIndexArray
     * @param value
     * @param list
     * @param i
     */
    private static List<NormalizeRow> updateNormalizedSurrogatesWithOriginal(
            List<Int2ObjectMap<List<int[]>>> listOfMapsOfDimValues,
            List<Integer> normalizedStartingIndexArray, int i, List<NormalizeRow> normList) {
        List<NormalizeRow> list =
                new ArrayList<NormalizeRow>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for (NormalizeRow value : normList) {
            NormalizeRow valueTemp = value.getCopy();
            // Abhigyan
            Int2ObjectMap<List<int[]>> mapOfDimValues = listOfMapsOfDimValues.get(i);
            // List<int[]> list2 =
            // listOfMapsOfDimValues.get(value[normalizedArray[i]]);
            Integer startIndex =
                    normalizedStartingIndexArray.get(i);//CHECKSTYLE:OFF    Approval No:Approval-289
            List<int[]> normValList = mapOfDimValues.get((int) value.key[startIndex]);
            // int[] normalizIndex = normalizedIndexes[i];//CHECKSTYLE:ON
            /*
             * if(normValList != null) {
             */
            if (normValList != null) {
                for (int[] vals : normValList) {
                    int k = startIndex;
                    /*
                     * for(int j2 = 0;j2 < normalizIndex.length;j2++) { valueTemp[k]
                     * = vals[normalizIndex[j2]]; k++; }
                     */
                    // Abhigyan
                    for (int j = 0;
                         j < vals.length - 1; j++) {//CHECKSTYLE:OFF    Approval No:Approval-290
                        valueTemp.key[k] = vals[j];
                        k++;
                    }//CHECKSTYLE:ON
                    list.add(valueTemp);
                    valueTemp = value.getCopy();
                }
            }
            /*
             * } else { System.out.println("Just to get a debug point."); }
             */
        }
        return list;
    }

    /**
     * NormalizeRow
     *
     * @author R00900208
     */
    private static final class NormalizeRow {
        private int[] key;

        private MeasureAggregator[] value;

        private NormalizeRow(int[] key, MeasureAggregator[] value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Returns the copy of the row.
         *
         * @return row.
         */
        public NormalizeRow getCopy() {
            int[] copyKey = key.clone();
            MeasureAggregator[] valueCopy = new MeasureAggregator[value.length];
            for (int i = 0; i < valueCopy.length; i++) {
                valueCopy[i] = value[i].getCopy();
            }
            return new NormalizeRow(copyKey, valueCopy);
        }
    }

}
