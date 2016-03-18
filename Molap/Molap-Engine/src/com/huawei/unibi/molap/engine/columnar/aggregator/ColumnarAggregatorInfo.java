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

package com.huawei.unibi.molap.engine.columnar.aggregator;

import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.engine.aggregator.CustomMolapAggregateExpression;
import com.huawei.unibi.molap.engine.aggregator.dimension.DimensionAggregatorInfo;
import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class ColumnarAggregatorInfo
{
    private int queryDimensionsLength;
    
    private Map<Integer,GenericQueryType> complexQueryDims;
    
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
    private double[] uniqueValue;
    
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
    private double[] msrMinValue;
    
    /**
     * measurIndex
     */
    private int measureStartIndex;
    
    /**
     * dimensionAggInfos
     */
    protected List<DimensionAggregatorInfo> dimensionAggInfos;
    
    /**
     * slices
     */
    private List<InMemoryCube> slices; 
    
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
    private List<CustomMolapAggregateExpression> expressions;
    
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
    private double[] msrDefaultValue;

    /**
     * 
     */
    private boolean[] highCardinalityTypes;
    
  
    /**
     * @return the cubeUniqueName
     */
    public String getCubeUniqueName()
    {
        return cubeUniqueName;
    }

    /**
     * @param cubeUniqueName the cubeUniqueName to set
     */
    public void setCubeUniqueName(String cubeUniqueName)
    {
        this.cubeUniqueName = cubeUniqueName;
    }

    /**
     * @return the countMsrIndex
     */
    public int getCountMsrIndex()
    {
        return countMsrIndex;
    }

    /**
     * @param countMsrIndex the countMsrIndex to set
     */
    public void setCountMsrIndex(int countMsrIndex)
    {
        this.countMsrIndex = countMsrIndex;
    }
    
    public void setQueryDimensionsLength(int queryDimensionsLength)
    {
        this.queryDimensionsLength = queryDimensionsLength;
    }
    
    public int getQueryDimensionsLength()
    {
        return queryDimensionsLength;
    }
    
    public Dimension[] getDimensions()
    {
        return dimensions;
    }

    public void setDimensions(Dimension[] dimensions)
    {
        this.dimensions = dimensions;
    }

    public Map<Integer, GenericQueryType> getComplexQueryDims()
    {
        return complexQueryDims;
    }

    public void setComplexQueryDims(Map<Integer, GenericQueryType> complexQueryDims)
    {
        this.complexQueryDims = complexQueryDims;
    }
    
    /**
     * @return the avgMsrIndexes
     */
    public int[] getAvgMsrIndexes()
    {
        return avgMsrIndexes;
    }

    /**
     * @param avgMsrIndexes the avgMsrIndexes to set
     */
    public void setAvgMsrIndexes(int[] avgMsrIndexes)
    {
        this.avgMsrIndexes = avgMsrIndexes;
    }

    /**
     * @return the measureOrdinal
     */
    public int[] getMeasureOrdinal()
    {
        return measureOrdinal;
    }

    /**
     * @param measureOrdinal the measureOrdinal to set
     */
    public void setMeasureOrdinal(int[] measureOrdinal)
    {
        this.measureOrdinal = measureOrdinal;
    }

    /**
     * @return the uniqueValue
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }

    /**
     * @param uniqueValue the uniqueValue to set
     */
    public void setUniqueValue(double[] uniqueValue)
    {
        this.uniqueValue = uniqueValue;
    }

    public int getLimit()
    {
        return limit;
    }

    public void setLimit(int limit)
    {
        this.limit = limit;
    }

    public KeyGenerator getLatestKeyGenerator()
    {
        return latestKeyGenerator;
    }

    public void setLatestKeyGenerator(KeyGenerator latestKeyGenerator)
    {
        this.latestKeyGenerator = latestKeyGenerator;
    }

    public byte[] getActualMaxKeyBasedOnDimensions()
    {
        return actualMaxKeyBasedOnDimensions;
    }

   public void setActalMaskedByteRanges(int[] actalMaskedByteRanges)
    {
        this.actalMaskedByteRanges = actalMaskedByteRanges;
    }

    public int getActualMaskedKeyByteSize()
    {
        return actualMaskedKeyByteSize;
    }

    public void setActualMaskedKeyByteSize(int actualMaskedKeyByteSize)
    {
        this.actualMaskedKeyByteSize = actualMaskedKeyByteSize;
    }
	
	 public List<DimensionAggregatorInfo> getDimensionAggInfos()
    {
        return dimensionAggInfos;
    }

    public void setDimensionAggInfos(List<DimensionAggregatorInfo> dimensionAggInfos)
    {
        this.dimensionAggInfos = dimensionAggInfos;
    }

    public String[] getAggType()
    {
        return aggType;
    }

    public void setAggType(String[] aggType)
    {
        this.aggType = aggType;
    }

    public double[] getMsrMinValue()
    {
        return msrMinValue;
    }

    public void setMsrMinValue(double[] msrMinValue)
    {
        this.msrMinValue = msrMinValue;
    }

    public List<InMemoryCube> getSlices()
    {
        return slices;
    }

    public void setSlices(List<InMemoryCube> slices)
    {
        this.slices = slices;
    }

    public int getCurrentSliceIndex()
    {
        return currentSliceIndex;
    }

    public void setCurrentSliceIndex(int currentSliceIndex)
    {
        this.currentSliceIndex = currentSliceIndex;
    }

    public int getMeasureStartIndex()
    {
        return measureStartIndex;
    }

    public void setMeasureStartIndex(int measureStartIndex)
    {
        this.measureStartIndex = measureStartIndex;
    }

    public boolean isNonFilterQuery()
    {
        return isNonFilterQuery;
    }

    public void setNonFilterQuery(boolean isNonFilterQuery)
    {
        this.isNonFilterQuery = isNonFilterQuery;
    }
    
    public void setActualMaxKeyBasedOnDimensions(byte[] actualMaxKeyBasedOnDimensions)
    {
        this.actualMaxKeyBasedOnDimensions = actualMaxKeyBasedOnDimensions;
    }

    public int[] getActalMaskedByteRanges()
    {
        return actalMaskedByteRanges;
    }
    
    public List<CustomMolapAggregateExpression> getCustomExpressions()
    {
        return expressions;
    }

    public void setCustomExpressions(List<CustomMolapAggregateExpression> expressions)
    {
        this.expressions = expressions;
    }

    public int getExpressionStartIndex()
    {
        return expressionStartIndex;
    }

    public void setExpressionStartIndex(int expressionStartIndex)
    {
        this.expressionStartIndex = expressionStartIndex;
    }

    public boolean[] getIsMeasureExistis()
    {
        return isMeasureExistis;
    }

    public void setIsMeasureExistis(boolean[] isMeasureExistis)
    {
        this.isMeasureExistis = isMeasureExistis;
    }

    public double[] getMsrDefaultValue()
    {
        return msrDefaultValue;
    }

    public void setMsrDefaultValue(double[] msrDefaultValue)
    {
        this.msrDefaultValue = msrDefaultValue;
    }

    /**
     * setHighCardinalityType.
     * @param highCardinalityTypes
     */
    public void setHighCardinalityType(boolean[] highCardinalityTypes)
    {
       this.highCardinalityTypes=highCardinalityTypes;
        
    }
    /**
     * getHighCardinalityTypes.
     * @return
     */
    public boolean[] getHighCardinalityTypes()
    {
        return highCardinalityTypes;
    }

}
