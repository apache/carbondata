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

package org.carbondata.query.columnar.aggregator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.datastorage.InMemoryTable;

public class ColumnarAggregatorInfo {
    /**
     * dimensionAggInfos
     */
    protected List<DimensionAggregatorInfo> dimensionAggInfos;
    /**
     * array of sql datatypes of mesaures and dimensions
     */
    protected SqlStatement.Type[] dataTypes;
    private int queryDimensionsLength;
    private Map<Integer, GenericQueryType> complexQueryDims;
    private Dimension[] dimensions;
    /**
     * slice
     */
    private String cubeUniqueName;
    /**
     * avgMsrIndexes
     */
    private int[] avgMsrIndexes;
    /**
     * countMsrIndex
     */
    private int countMsrIndex;
    /**
     * Measures ordinal
     */
    private int[] measureOrdinal;
    /**
     * uniqueValue
     */
    private Object[] uniqueValue;
    /**
     * limit
     */
    private int limit;
    /**
     * latestKeyGenerator
     */
    private KeyGenerator latestKeyGenerator;
    /**
     * actualMaxKeyBasedOnDimensions
     */
    private byte[] actualMaxKeyBasedOnDimensions;
    /**
     * actalMaskedByteRanges
     */
    private int[] actalMaskedByteRanges;
    /**
     * actualMaskedKeyByteSize
     */
    private int actualMaskedKeyByteSize;
    /**
     * aggType
     */
    private String[] aggType;
    /**
     * minValue
     */
    private Object[] msrMinValue;
    /**
     * measurIndex
     */
    private int measureStartIndex;
    /**
     * slices
     */
    private List<InMemoryTable> slices;
    /**
     * currentSliceIndex
     */
    private int currentSliceIndex;
    /**
     * isNonFilterQuery
     */
    private boolean isNonFilterQuery;
    /**
     *
     */
    private List<CustomCarbonAggregateExpression> expressions;
    /**
     * expressionStartIndex
     */
    private int expressionStartIndex;
    /**
     * isMeasureExistis
     */
    private boolean[] isMeasureExistis;
    /**
     * msrDefaultValue
     */

    private Object[] msrDefaultValue;
    /**
     *
     */
    private boolean[] noDictionaryTypes;
    private HashMap<Integer, Integer> measureOrdinalMap;

    /**
     * @return the cubeUniqueName
     */
    public String getCubeUniqueName() {
        return cubeUniqueName;
    }

    /**
     * @param cubeUniqueName the cubeUniqueName to set
     */
    public void setCubeUniqueName(String cubeUniqueName) {
        this.cubeUniqueName = cubeUniqueName;
    }

    /**
     * @return the countMsrIndex
     */
    public int getCountMsrIndex() {
        return countMsrIndex;
    }

    /**
     * @param countMsrIndex the countMsrIndex to set
     */
    public void setCountMsrIndex(int countMsrIndex) {
        this.countMsrIndex = countMsrIndex;
    }

    public int getQueryDimensionsLength() {
        return queryDimensionsLength;
    }

    public void setQueryDimensionsLength(int queryDimensionsLength) {
        this.queryDimensionsLength = queryDimensionsLength;
    }

    public Dimension[] getDimensions() {
        return dimensions;
    }

    public void setDimensions(Dimension[] dimensions) {
        this.dimensions = dimensions;
    }

    public Map<Integer, GenericQueryType> getComplexQueryDims() {
        return complexQueryDims;
    }

    public void setComplexQueryDims(Map<Integer, GenericQueryType> complexQueryDims) {
        this.complexQueryDims = complexQueryDims;
    }

    /**
     * @return the avgMsrIndexes
     */
    public int[] getAvgMsrIndexes() {
        return avgMsrIndexes;
    }

    /**
     * @param avgMsrIndexes the avgMsrIndexes to set
     */
    public void setAvgMsrIndexes(int[] avgMsrIndexes) {
        this.avgMsrIndexes = avgMsrIndexes;
    }

    /**
     * @return the measureOrdinal
     */
    public int[] getMeasureOrdinal() {
        return measureOrdinal;
    }

    /**
     * @param measureOrdinal the measureOrdinal to set
     */
    public void setMeasureOrdinal(int[] measureOrdinal) {
        this.measureOrdinal = measureOrdinal;
    }

    /**
     * @return the uniqueValue
     */
    public Object[] getUniqueValue() {
        return uniqueValue;
    }

    /**
     * @param uniqueValue the uniqueValue to set
     */
    public void setUniqueValue(Object[] uniqueValue) {
        this.uniqueValue = uniqueValue;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public KeyGenerator getLatestKeyGenerator() {
        return latestKeyGenerator;
    }

    public void setLatestKeyGenerator(KeyGenerator latestKeyGenerator) {
        this.latestKeyGenerator = latestKeyGenerator;
    }

    public byte[] getActualMaxKeyBasedOnDimensions() {
        return actualMaxKeyBasedOnDimensions;
    }

    public void setActualMaxKeyBasedOnDimensions(byte[] actualMaxKeyBasedOnDimensions) {
        this.actualMaxKeyBasedOnDimensions = actualMaxKeyBasedOnDimensions;
    }

    public int getActualMaskedKeyByteSize() {
        return actualMaskedKeyByteSize;
    }

    public void setActualMaskedKeyByteSize(int actualMaskedKeyByteSize) {
        this.actualMaskedKeyByteSize = actualMaskedKeyByteSize;
    }

    public List<DimensionAggregatorInfo> getDimensionAggInfos() {
        return dimensionAggInfos;
    }

    public void setDimensionAggInfos(List<DimensionAggregatorInfo> dimensionAggInfos) {
        this.dimensionAggInfos = dimensionAggInfos;
    }

    public String[] getAggType() {
        return aggType;
    }

    public void setAggType(String[] aggType) {
        this.aggType = aggType;
    }

    public Object[] getMsrMinValue() {
        return msrMinValue;
    }

    public void setMsrMinValue(Object[] msrMinValue) {
        this.msrMinValue = msrMinValue;
    }

    public List<InMemoryTable> getSlices() {
        return slices;
    }

    public void setSlices(List<InMemoryTable> slices) {
        this.slices = slices;
    }

    public int getCurrentSliceIndex() {
        return currentSliceIndex;
    }

    public void setCurrentSliceIndex(int currentSliceIndex) {
        this.currentSliceIndex = currentSliceIndex;
    }

    public int getMeasureStartIndex() {
        return measureStartIndex;
    }

    public void setMeasureStartIndex(int measureStartIndex) {
        this.measureStartIndex = measureStartIndex;
    }

    public boolean isNonFilterQuery() {
        return isNonFilterQuery;
    }

    public void setNonFilterQuery(boolean isNonFilterQuery) {
        this.isNonFilterQuery = isNonFilterQuery;
    }

    public int[] getActalMaskedByteRanges() {
        return actalMaskedByteRanges;
    }

    public void setActalMaskedByteRanges(int[] actalMaskedByteRanges) {
        this.actalMaskedByteRanges = actalMaskedByteRanges;
    }

    public List<CustomCarbonAggregateExpression> getCustomExpressions() {
        return expressions;
    }

    public void setCustomExpressions(List<CustomCarbonAggregateExpression> expressions) {
        this.expressions = expressions;
    }

    public int getExpressionStartIndex() {
        return expressionStartIndex;
    }

    public void setExpressionStartIndex(int expressionStartIndex) {
        this.expressionStartIndex = expressionStartIndex;
    }

    public boolean[] getIsMeasureExistis() {
        return isMeasureExistis;
    }

    public void setIsMeasureExistis(boolean[] isMeasureExistis) {
        this.isMeasureExistis = isMeasureExistis;
    }

    public Object[] getMsrDefaultValue() {
        return msrDefaultValue;
    }

    public void setMsrDefaultValue(Object[] msrDefaultValue) {
        this.msrDefaultValue = msrDefaultValue;
    }

    public SqlStatement.Type[] getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(SqlStatement.Type[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    /**
     * setNoDictionaryType.
     *
     * @param noDictionaryTypes
     */
    public void setNoDictionaryType(boolean[] noDictionaryTypes) {
        this.noDictionaryTypes = noDictionaryTypes;

    }

    /**
     * getNoDictionaryTypes.
     *
     * @return
     */
    public boolean[] getNoDictionaryTypes() {
        return noDictionaryTypes;
    }

    public HashMap<Integer, Integer> getMeasureOrdinalMap() {
        return measureOrdinalMap;
    }

    public void setMeasureOrdinalMap(HashMap<Integer, Integer> measureOrdinal) {
        this.measureOrdinalMap = measureOrdinal;
    }

}
