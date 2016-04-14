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

package org.carbondata.query.util;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.carbon.SqlStatement.Type;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.query.complex.querytypes.ArrayQueryType;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.complex.querytypes.PrimitiveQueryType;
import org.carbondata.query.complex.querytypes.StructQueryType;
import org.carbondata.query.datastorage.TableDataStore;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.datastorage.MemberStore;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.schema.metadata.SliceUniqueValueInfo;

public final class QueryExecutorUtility {

    private QueryExecutorUtility() {

    }

    public static Object[] updateUniqueForSlices(String factTable, boolean isAgg,
            List<InMemoryTable> slices, SqlStatement.Type[] dataTypes) {
        List<SliceUniqueValueInfo> sliceUniqueValueInfos =
                new ArrayList<SliceUniqueValueInfo>(null != slices ? slices.size() : 0);

        Object[] uniqueValue = null;
        processUniqueAndMinValueInfo(factTable, sliceUniqueValueInfos, true, isAgg, slices);
        if (sliceUniqueValueInfos.size() > 0) {
            uniqueValue = mergerSliceUniqueValueInfo(sliceUniqueValueInfos, dataTypes);
        }
        return uniqueValue;
    }

    public static Object[] getMinValueOfSlices(String factTable, boolean isAgg,
            List<InMemoryTable> slices, SqlStatement.Type[] dataTypes) {
        List<SliceUniqueValueInfo> sliceMinValueInfos =
                new ArrayList<SliceUniqueValueInfo>(null != slices ? slices.size() : 0);
        processUniqueAndMinValueInfo(factTable, sliceMinValueInfos, false, isAgg, slices);
        Object[] minValues = new Object[0];
        if (sliceMinValueInfos.size() > 0) {
            minValues = mergerSliceUniqueValueInfo(sliceMinValueInfos, dataTypes);
        }
        return minValues;
    }

    private static void processUniqueAndMinValueInfo(String factTable,
            List<SliceUniqueValueInfo> sliceUniqueValueInfos, boolean uniqueValue, boolean isAgg,
            List<InMemoryTable> slices) {
        SliceUniqueValueInfo sliceUniqueValueInfo = null;
        SliceMetaData sliceMataData = null;
        if (slices != null) {
            for (int i = 0; i < slices.size(); i++) {
                TableDataStore dataCache = slices.get(i).getDataCache(factTable);
                if (null != dataCache) {
                    sliceMataData = slices.get(i).getRsStore().getSliceMetaCache(factTable);
                    Object[] currentUniqueValue = null;
                    SqlStatement.Type[] dataType;
                    if (uniqueValue) {
                        currentUniqueValue = slices.get(i).getDataCache(factTable).getUniqueValue();
                    } else {
                        if (isAgg) {
                            currentUniqueValue =
                                    slices.get(i).getDataCache(factTable).getMinValueFactForAgg();
                        } else {
                            currentUniqueValue =
                                    slices.get(i).getDataCache(factTable).getMinValue();
                        }
                    }
                    sliceUniqueValueInfo = new SliceUniqueValueInfo();
                    sliceUniqueValueInfo.setCols(sliceMataData.getMeasures());
                    sliceUniqueValueInfo.setUniqueValue(currentUniqueValue);
                    sliceUniqueValueInfos.add(sliceUniqueValueInfo);
                }
            }
        }
    }

    /**
     * @param sliceUniqueValueInfos
     */
    private static Object[] mergerSliceUniqueValueInfo(
            List<SliceUniqueValueInfo> sliceUniqueValueInfos, SqlStatement.Type[] dataTypes) {
        int maxInfoIndex = 0;
        int lastMaxValue = 0;
        for (int i = 0; i < sliceUniqueValueInfos.size(); i++) {
            if (sliceUniqueValueInfos.get(i).getLength() > lastMaxValue) {
                lastMaxValue = sliceUniqueValueInfos.get(i).getLength();
                maxInfoIndex = i;
            }
        }
        SliceUniqueValueInfo sliceUniqueValueInfo = sliceUniqueValueInfos.get(maxInfoIndex);
        Object[] maxSliceUniqueValue = sliceUniqueValueInfo.getUniqueValue();
        String[] cols = null;
        Object[] currentUniqueValue = null;
        for (int i = 0; i < sliceUniqueValueInfos.size(); i++) {
            if (i == maxInfoIndex) {
                continue;
            }
            cols = sliceUniqueValueInfos.get(i).getCols();
            currentUniqueValue = sliceUniqueValueInfos.get(i).getUniqueValue();
            for (int j = 0; j < cols.length; j++) {
                switch (dataTypes[j]) {
                case LONG:
                    maxSliceUniqueValue[j] =
                            (long) maxSliceUniqueValue[j] > (long) currentUniqueValue[j] ?
                                    currentUniqueValue[j] :
                                    maxSliceUniqueValue[j];
                    break;
                case DECIMAL:
                    maxSliceUniqueValue[j] = ((BigDecimal) maxSliceUniqueValue[j])
                            .compareTo((BigDecimal) currentUniqueValue[j]) > 0 ?
                            currentUniqueValue[j] :
                            maxSliceUniqueValue[j];
                    break;
                default:
                    maxSliceUniqueValue[j] =
                            (double) maxSliceUniqueValue[j] > (double) currentUniqueValue[j] ?
                                    currentUniqueValue[j] :
                                    maxSliceUniqueValue[j];
                }
            }
        }
        return maxSliceUniqueValue;
    }

    /**
     * et measure indexes with agg type
     *
     * @param msrs
     * @param aggType
     * @return List<Integer>
     */
    public static List<Integer> getMeasureIndexes(List<Measure> msrs, String aggType,
            List<Integer> integers) {
        int i = 0;
        for (Measure msr : msrs) {
            if (msr.getAggName().equals(aggType)) {
                integers.add(i);
            }
            i++;
        }
        return integers;
    }

    /**
     * @param msrs
     * @param allMsrs
     * @return
     */
    public static List<Measure> getOriginalMeasures(List<Measure> msrs, List<Measure> allMsrs) {
        List<Measure> updated =
                new ArrayList<CarbonMetadata.Measure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (Measure currMsr : msrs) {
            for (Measure orgMsr : allMsrs) {
                if (currMsr.getOrdinal() == orgMsr.getOrdinal()) {
                    updated.add(orgMsr);
                    break;
                }
            }
        }
        return updated;
    }

    public static boolean updateFilterForOlderSlice(Map<Dimension, List<Integer>> dimensionFilter,
            Dimension[] currentDimeTables, List<InMemoryTable> slices) {
        if (dimensionFilter.size() < 1) {
            return true;
        }
        boolean isExecutionRequired = false;
        List<Entry<Dimension, List<Integer>>> entryList =
                new ArrayList<Entry<Dimension, List<Integer>>>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (Entry<Dimension, List<Integer>> entry : dimensionFilter.entrySet()) {
            boolean isFound = false;
            Dimension dim = entry.getKey();
            for (int i = 0; i < currentDimeTables.length; i++) {
                if (dim.getColName().equals(currentDimeTables[i].getColName())) {
                    isExecutionRequired = true;
                    isFound = true;
                    break;

                }
            }
            if (!isFound) {
                entryList.add(entry);
            }
        }
        for (Entry<Dimension, List<Integer>> entry : entryList) {
            List<Integer> values = entry.getValue();
            for (Integer value : values) {
                if (value != 1) {
                    isExecutionRequired = false;
                    break;
                }
            }
        }
        return isExecutionRequired;
    }

    public static boolean updateMsrFilterForOlderSlice(Map<Measure, double[]> measureFilters,
            SliceMetaData sliceMetaData) {
        if (measureFilters.size() < 1) {
            return true;
        }
        String[] newMeasures = sliceMetaData.getNewMeasures();
        double[] newMsrDfts = sliceMetaData.getNewMsrDfts();
        boolean isExecutionRequired = true;
        for (Entry<Measure, double[]> entry : measureFilters.entrySet()) {
            Measure key = entry.getKey();
            for (int i = 0; i < newMeasures.length; i++) {
                if (key.getColName().equals(newMeasures[i])) {
                    double[] filterValue = entry.getValue();
                    if (!checkDefaultValueIsPresent(newMsrDfts, filterValue)) {
                        isExecutionRequired = false;
                        break;
                    }
                }
            }
        }
        return isExecutionRequired;
    }

    private static boolean checkDefaultValueIsPresent(double[] newMsrDfts, double[] value) {
        boolean isExecutionRequired = true;
        for (int j = 0; j < newMsrDfts.length; j++) {
            for (int k = 0; k < value.length; k++) {
                if (Double.compare(value[k], newMsrDfts[j]) != 0) {
                    isExecutionRequired = false;
                    break;
                }
            }
        }
        return isExecutionRequired;
    }

    public static int[][] getMaskedByteRangeForSorting(Dimension[] queryDimensions,
            KeyGenerator generator, int[] maskedRanges) {
        int[][] dimensionCompareIndex = new int[queryDimensions.length][];
        int index = 0;
        for (int i = 0; i < queryDimensions.length; i++) {
            Set<Integer> integers = new TreeSet<Integer>();
            if (queryDimensions[i].isNoDictionaryDim()) {
                continue;
            }
            int[] range = generator.getKeyByteOffsets(queryDimensions[i].getOrdinal());
            for (int j = range[0]; j <= range[1]; j++) {
                integers.add(j);
            }
            dimensionCompareIndex[index] = new int[integers.size()];
            int j = 0;
            for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
                Integer integer = (Integer) iterator.next();
                dimensionCompareIndex[index][j++] = integer.intValue();
            }
            index++;
        }

        for (int i = 0; i < dimensionCompareIndex.length; i++) {
            if (null == dimensionCompareIndex[i]) {
                continue;
            }
            int[] range = dimensionCompareIndex[i];
            if (null != range) {
                for (int j = 0; j < range.length; j++) {
                    for (int k = 0; k < maskedRanges.length; k++) {
                        if (range[j] == maskedRanges[k]) {
                            range[j] = k;
                            break;
                        }
                    }
                }
            }

        }

        return dimensionCompareIndex;
    }

    public static int[][] getMaskedByteRangeForSorting(Dimension[] queryDimensions,
            KeyGenerator generator, int[] maskedRanges, HybridStoreModel hybridStoreModel) {

        int[][] dimensionCompareIndex = new int[queryDimensions.length][];
        int index = 0;
        for (int i = 0; i < queryDimensions.length; i++) {
            if (queryDimensions[i].isNoDictionaryDim()) {
                continue;
            }
            Set<Integer> integers = new TreeSet<Integer>();

            int[] range = generator.getKeyByteOffsets(
                    hybridStoreModel.getMdKeyOrdinal(queryDimensions[i].getOrdinal()));

            for (int j = range[0]; j <= range[1]; j++) {
                integers.add(j);
            }
            dimensionCompareIndex[index] = new int[integers.size()];
            int j = 0;
            for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
                Integer integer = (Integer) iterator.next();
                dimensionCompareIndex[index][j++] = integer.intValue();
            }
            index++;
        }

        for (int i = 0; i < dimensionCompareIndex.length; i++) {
            int[] range = dimensionCompareIndex[i];
            for (int j = 0; j < range.length; j++) {
                for (int k = 0; k < maskedRanges.length; k++) {
                    if (range[j] == maskedRanges[k]) {
                        range[j] = k;
                        break;
                    }
                }
            }
        }
        return dimensionCompareIndex;

    }

    public static byte[][] getMaksedKeyForSorting(Dimension[] queryDimensions,
            KeyGenerator generator, int[][] dimensionCompareIndex, int[] maskedRanges)
            throws QueryExecutionException {
        byte[][] maskedKey = new byte[queryDimensions.length][];
        byte[] mdKey = null;
        long[] key = null;
        byte[] maskedMdKey = null;
        try {
            if (null != dimensionCompareIndex) {
                for (int i = 0; i < dimensionCompareIndex.length; i++) {
                    if (null == dimensionCompareIndex[i]) {
                        continue;
                    }
                    key = new long[generator.getDimCount()];
                    maskedKey[i] = new byte[dimensionCompareIndex[i].length];
                    key[queryDimensions[i].getOrdinal()] = Long.MAX_VALUE;
                    mdKey = generator.generateKey(key);
                    maskedMdKey = new byte[maskedRanges.length];
                    for (int k = 0; k
                            < maskedMdKey.length; k++) { // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
                        maskedMdKey[k] = mdKey[maskedRanges[k]];
                    }
                    for (int j = 0; j < dimensionCompareIndex[i].length; j++) {
                        maskedKey[i][j] = maskedMdKey[dimensionCompareIndex[i][j]];
                    }// CHECKSTYLE:ON

                }
            }
        } catch (KeyGenException e) {
            throw new QueryExecutionException(e);
        }
        return maskedKey;
    }

    //@TODO need to handle for restructuring scenario 
    public static int[] getSelectedDimnesionIndex(Dimension[] queryDims) {
        //        int[] selectedDimsIndex= new int[queryDims.length];
        //        for(int i = 0;i < queryDims.length;i++)
        //        {
        //            selectedDimsIndex[i]=queryDims[i].getOrdinal();
        //        }
        //        Arrays.sort(selectedDimsIndex);
        //        return selectedDimsIndex;
        // updated for block index size with complex types
        Set<Integer> allQueryDimension =
                new LinkedHashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        //                new TreeSet<Integer>(new Comparator<Integer>(){
        //            public int compare(Integer a, Integer b){
        //                return a.compareTo(b);
        //            }
        //        });
        Map<Integer, Integer> primitiveCols = new LinkedHashMap<Integer, Integer>();
        int index = 0;
        for (int i = 0; i < queryDims.length; i++) {
            if (queryDims[i].getAllApplicableDataBlockIndexs().length > 1) {
                for (int eachBlockIndex : queryDims[i].getAllApplicableDataBlockIndexs()) {
                    allQueryDimension.add(eachBlockIndex);
                    index++;
                }
            } else {
                allQueryDimension.add(queryDims[i].getOrdinal());
                primitiveCols.put(index++, queryDims[i].getOrdinal());
            }
        }
        int[] indexArray = convertIntegerArrayToInt(
                allQueryDimension.toArray(new Integer[allQueryDimension.size()]));

        //Sorting only primitives cols for MDKey Generation.
        Map<Integer, Integer> sortedPrimitiveCols = sortByComparator(primitiveCols);
        List<Entry<Integer, Integer>> unordered =
                new ArrayList<Entry<Integer, Integer>>(primitiveCols.entrySet());
        List<Entry<Integer, Integer>> ordered =
                new ArrayList<Entry<Integer, Integer>>(sortedPrimitiveCols.entrySet());

        for (int i = 0; i < unordered.size(); i++) {
            indexArray[unordered.get(i).getKey()] = ordered.get(i).getValue();
        }

        return indexArray;
    }

    private static Map<Integer, Integer> sortByComparator(Map<Integer, Integer> unsortMap) {

        List<Entry<Integer, Integer>> list =
                new ArrayList<Entry<Integer, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<Integer, Integer>>() {
            @Override
            public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<Integer, Integer> sortedMap = new LinkedHashMap<Integer, Integer>();
        for (Entry<Integer, Integer> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    public static Map<Integer, GenericQueryType> getQueryComplexTypes(Dimension[] queryDimensions,
            Map<String, GenericQueryType> complexDimensionsMap) {
        Map<Integer, GenericQueryType> queryComplexMap = new HashMap<Integer, GenericQueryType>();
        for (Dimension d : queryDimensions) {
            GenericQueryType complexType = complexDimensionsMap.get(d.getHierName());
            if (complexType != null) {
                queryComplexMap
                        .put(d.getDataBlockIndex(), complexDimensionsMap.get(d.getHierName()));
            }
        }
        return queryComplexMap;
    }

    public static Map<Integer, GenericQueryType> getAllComplexTypesBlockStartIndex(
            Map<String, GenericQueryType> complexDimensionsMap) {
        Map<Integer, GenericQueryType> queryComplexMap = new HashMap<Integer, GenericQueryType>();
        for (Entry<String, GenericQueryType> d : complexDimensionsMap.entrySet()) {
            queryComplexMap.put(d.getValue().getBlockIndex(), d.getValue());
        }
        return queryComplexMap;
    }

    /**
     * @param cube
     * @return
     */
    public static Map<String, GenericQueryType> getComplexDimensionsMap(
            Dimension[] currentDimTables) {
        Map<String, GenericQueryType> complexTypeMap = new HashMap<String, GenericQueryType>();

        Map<String, ArrayList<Dimension>> complexDimensions =
                new HashMap<String, ArrayList<Dimension>>();
        for (int i = 0; i < currentDimTables.length; i++) {
            ArrayList<Dimension> dimensions =
                    complexDimensions.get(currentDimTables[i].getHierName());
            if (dimensions != null) {
                dimensions.add(currentDimTables[i]);
            } else {
                dimensions = new ArrayList<Dimension>();
                dimensions.add(currentDimTables[i]);
            }
            complexDimensions.put(currentDimTables[i].getHierName(), dimensions);
        }
        for (Entry<String, ArrayList<Dimension>> entry : complexDimensions.entrySet()) {
            if (entry.getValue().size() > 1) {
                Dimension dimZero = entry.getValue().get(0);
                GenericQueryType g = dimZero.getDataType().equals(SqlStatement.Type.ARRAY) ?
                        new ArrayQueryType(dimZero.getColName(), "", dimZero.getDataBlockIndex()) :
                        new StructQueryType(dimZero.getColName(), "", dimZero.getDataBlockIndex());
                complexTypeMap.put(dimZero.getColName(), g);
                for (int i = 1; i < entry.getValue().size(); i++) {
                    Dimension dim = entry.getValue().get(i);
                    switch (dim.getDataType()) {
                    case ARRAY:
                        g.addChildren(new ArrayQueryType(dim.getColName(), dim.getParentName(),
                                dim.getDataBlockIndex()));
                        break;
                    case STRUCT:
                        g.addChildren(new StructQueryType(dim.getColName(), dim.getParentName(),
                                dim.getDataBlockIndex()));
                        break;
                    default:
                        g.addChildren(new PrimitiveQueryType(dim.getColName(), dim.getParentName(),
                                dim.getDataBlockIndex(), dim.getDataType()));
                    }
                }
            }
        }

        return complexTypeMap;
    }

    /**
     * This method will get store index for each ordinal
     * by default all dimension part of row store, their index will be 0
     *
     * @param queryDims
     * @param hybridStoreModel
     * @return
     */
    public static int[] getSelectedDimensionStoreIndex(Dimension[] queryDims,
            HybridStoreModel hybridStoreModel) {
        //it can be possible that multiple queryDim will be part of row store and hence .if row store index is already added then its not required to add again.
        Set<Integer> selectedDimensionList = new HashSet<Integer>(queryDims.length);
        int NoDictionaryStartIndex = hybridStoreModel.getColumnStoreOrdinals().length;
        for (Dimension dimension : queryDims) {
            if (dimension.isNoDictionaryDim()) {
                selectedDimensionList.add(NoDictionaryStartIndex++);
            } else {
                int storeIndex = hybridStoreModel.getStoreIndex(dimension.getOrdinal());
                selectedDimensionList.add(storeIndex);
            }
        }
        for (int i = 0; i < queryDims.length; i++) {
            int storeIndex = hybridStoreModel.getStoreIndex(queryDims[i].getOrdinal());
            selectedDimensionList.add(storeIndex);
        }
        int[] selectedDimsIndex =
                QueryExecutorUtil.convertIntegerListToIntArray(selectedDimensionList);
        Arrays.sort(selectedDimsIndex);
        return selectedDimsIndex;
    }

    public static void getComplexDimensionsKeySize(
            Map<String, GenericQueryType> complexDimensionsMap, int[] dimensionCardinality) {
        int keyBlockSize[] = new MultiDimKeyVarLengthEquiSplitGenerator(
                CarbonUtil.getIncrementedCardinalityFullyFilled(dimensionCardinality), (byte) 1)
                .getBlockKeySize();
        for (Entry<String, GenericQueryType> entry : complexDimensionsMap.entrySet()) {
            entry.getValue().setKeySize(keyBlockSize);
        }
    }

    public static Map<String, Integer> getComplexQueryIndexes(Dimension[] queryDims,
            Dimension[] currentDimTables) {
        Map<String, Integer> colToDataMap = new HashMap<String, Integer>();
        //        boolean[] dimPresent = new boolean[currentDimTables.length];
        int index = 0;
        for (Dimension queryDim : queryDims) {
            for (int i = 0; i < currentDimTables.length; i++) {
                if (currentDimTables[i].getColName().equals(queryDim.getColName()) && (
                        currentDimTables[i].getDataType() == Type.ARRAY
                                || currentDimTables[i].getDataType() == Type.STRUCT)) {
                    //                    dimPresent[i] = true;
                    colToDataMap.put(currentDimTables[i].getColName(), index++);
                    break;
                }
            }
        }
        //        for(int i=0;i<dimPresent.length;i++)
        //        {
        //            if(dimPresent[i] == true)
        //            {
        //                colToDataMap.put(currentDimTables[i].getColName(), index++);
        //            }
        //        }
        return colToDataMap;
    }

    public static int[] getAllSelectedDiemnsion(Dimension[] queryDims,
            List<DimensionAggregatorInfo> dimAggInfo, List<Dimension> fromCustomExps) {
        //Updated to get multiple column blocks for complex types
        Set<Integer> allQueryDimension =
                new LinkedHashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < queryDims.length; i++) {
            if (queryDims[i].getAllApplicableDataBlockIndexs().length > 1) {
                for (int eachBlockIndex : queryDims[i].getAllApplicableDataBlockIndexs()) {
                    allQueryDimension.add(eachBlockIndex);
                }
            } else {
                allQueryDimension.add(queryDims[i].getOrdinal());
            }
        }
        for (int i = 0; i < dimAggInfo.size(); i++) {
            if (dimAggInfo.get(i).isDimensionPresentInCurrentSlice()) {
                if (dimAggInfo.get(i).getDim().getAllApplicableDataBlockIndexs().length > 1) {
                    for (int eachBlockIndex : dimAggInfo.get(i).getDim()
                            .getAllApplicableDataBlockIndexs()) {
                        allQueryDimension.add(eachBlockIndex);
                    }
                } else {
                    allQueryDimension.add(dimAggInfo.get(i).getDim().getOrdinal());
                }
            }
        }

        for (int i = 0; i < fromCustomExps.size(); i++) {
            if (fromCustomExps.get(i).getAllApplicableDataBlockIndexs().length > 1) {
                for (int eachBlockIndex : fromCustomExps.get(i).getAllApplicableDataBlockIndexs()) {
                    allQueryDimension.add(eachBlockIndex);
                }
            } else {
                allQueryDimension.add(fromCustomExps.get(i).getOrdinal());
            }
        }
        return convertIntegerArrayToInt(
                allQueryDimension.toArray(new Integer[allQueryDimension.size()]));
    }

    public static int[] getAllSelectedDiemnsionStoreIndex(Dimension[] queryDims,
            List<DimensionAggregatorInfo> dimAggInfo, List<Dimension> fromCustomExps,
            HybridStoreModel hybridStoreModel) {
        Set<Integer> allQueryDimension =
                new HashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < queryDims.length; i++) {
            allQueryDimension.add(hybridStoreModel.getStoreIndex(queryDims[i].getOrdinal()));
        }
        for (int i = 0; i < dimAggInfo.size(); i++) {
            if (dimAggInfo.get(i).isDimensionPresentInCurrentSlice()) {
                allQueryDimension.add(hybridStoreModel
                        .getStoreIndex(dimAggInfo.get(i).getDim().getOrdinal()));
            }
        }

        for (int i = 0; i < fromCustomExps.size(); i++) {
            allQueryDimension
                    .add(hybridStoreModel.getStoreIndex(fromCustomExps.get(i).getOrdinal()));
        }
        return convertIntegerArrayToInt(
                allQueryDimension.toArray(new Integer[allQueryDimension.size()]));
    }

    public static int[] getAllSelectedMeasureOrdinals(Measure[] queryMeasure,
            List<Measure> filterExpMeasures, String[] strings) {
        Set<Integer> allQueryMeasures =
                new HashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < queryMeasure.length; i++) {
            if (isNewMeasure(strings, queryMeasure[i]) > -1) {
                allQueryMeasures.add(queryMeasure[i].getOrdinal());
            }
        }
        for (int i = 0; i < filterExpMeasures.size(); i++) {
            if (isNewMeasure(strings, filterExpMeasures.get(i)) > -1) {
                allQueryMeasures.add(filterExpMeasures.get(i).getOrdinal());
            }
        }
        int[] convertIntegerArrayToInt = convertIntegerArrayToInt(
                allQueryMeasures.toArray(new Integer[allQueryMeasures.size()]));
        Arrays.sort(convertIntegerArrayToInt);
        return convertIntegerArrayToInt;
    }

    public static int[] convertIntegerArrayToInt(Integer[] integerArray) {
        int[] intArray = new int[integerArray.length];

        for (int i = 0; i < integerArray.length; i++) {
            intArray[i] = integerArray[i];
        }
        return intArray;
    }

    public static byte[] getMaskedKey(byte[] data, byte[] maxKey, int[] maskByteRanges,
            int byteCount) {
        // check masked key is null or not
        byte[] maskedKey = new byte[byteCount];
        int counter = 0;
        int byteRange = 0;
        for (int i = 0; i < byteCount; i++) {
            byteRange = maskByteRanges[i];
            if (byteRange != -1) {
                maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
            }
        }
        return maskedKey;
    }

    public static Member getMemberBySurrogateKey(Dimension columnName, int surrogate,
            List<InMemoryTable> slices) {
        MemberStore store = null;
        for (InMemoryTable slice : slices) {
            store = slice.getMemberCache(
                    columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName
                            .getDimName() + '_' + columnName.getHierName());
            if (null != store) {
                Member member = store.getMemberByID(surrogate);
                if (member != null) {
                    return member;
                }
            }
        }
        return null;
    }

    public static Member getActualMemberBySortedKey(Dimension columnName, int surrogate,
            List<InMemoryTable> slices) {
        MemberStore store = null;
        for (InMemoryTable slice : slices) {
            store = slice.getMemberCache(
                    columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName
                            .getDimName() + '_' + columnName.getHierName());
            if (null != store) {
                Member member = store.getActualKeyFromSortedIndex(surrogate);
                if (member != null) {
                    return member;
                }
            }
        }
        return null;
    }

    //    public static Member getMemberBySortIndex(Dimension columnName, int surrogate,List<InMemoryCube> slices)
    //    {
    //        for(InMemoryCube slice : slices)
    //        {
    //            Member member = slice.getMemberCache(
    //                    columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName.getDimName() + '_'
    //                            + columnName.getHierName()).getMemberByID(surrogate);
    //            if(member != null)
    //            {
    //                return member;
    //            }
    //        }
    //        return null;
    //    }

    public static Member getMemberBySurrogateKey(Dimension columnName, int surrogate,
            List<InMemoryTable> slices, int currentSliceIndex) {
        MemberStore store = null;
        for (int i = 0; i <= currentSliceIndex; i++) {
            store = slices.get(i).getMemberCache(
                    columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName
                            .getDimName() + '_' + columnName.getHierName());
            if (null != store) {
                Member member = store.getMemberByID(surrogate);
                if (member != null) {
                    return member;
                }
            }
        }
        return null;
    }

    public static int isNewDimension(String[] dimensions, Dimension currDimension) {
        if (null == dimensions || dimensions.length < 1) {
            return -1;
        }
        for (int i = 0; i < dimensions.length; i++) {
            if (dimensions[i].equals(currDimension.getActualTableName() + '_' + currDimension
                    .getColName())) {
                return i;
            }
        }
        return -1;
    }

    public static int isNewMeasure(String[] measures, Measure currMeasure) {
        if (null == measures || measures.length < 1) {
            return -1;
        }
        for (int i = 0; i < measures.length; i++) {
            if (measures[i].equals(currMeasure.getColName())) {
                return i;
            }
        }
        return -1;
    }

    public static byte[] fillSortedDimensions(Dimension[] sortedDimensions,
            Dimension[] queryDimensions) {
        byte[] sortedDims = new byte[queryDimensions.length];
        for (int j = 0; j < queryDimensions.length; j++) {
            for (Dimension dimension : sortedDimensions) {
                if (dimension.equals(queryDimensions[j])) {
                    sortedDims[j] = 1;
                }
            }
        }
        return sortedDims;
    }

}
