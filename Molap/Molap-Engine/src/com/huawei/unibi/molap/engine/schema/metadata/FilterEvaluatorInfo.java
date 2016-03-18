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

package com.huawei.unibi.molap.engine.schema.metadata;

import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.impl.QueryFilterInfo;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.vo.HybridStoreModel;

public class FilterEvaluatorInfo
{
    private List<InMemoryCube> slices;

    private KeyGenerator keyGenerator;

    private int currentSliceIndex;

    private String factTableName;
    
    private QueryFilterInfo info;
    
    private String[] newDimension;
    
    private Dimension[] dimensions;
    
    private String[] newMeasures;
    
    private double[] newDefaultValues;
    
    private int[] newDimensionSurrogates;
    
    private String[] newDimensionDefaultValue;
    
    private Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex; 

    private HybridStoreModel hybridStoreModel;
   
    public Dimension[] getDimensions()
    {
        return dimensions;
    }

    public void setDimensions(Dimension[] dimensions)
    {
        this.dimensions = dimensions;
    }
    
    public Map<Integer, GenericQueryType> getComplexTypesWithBlockStartIndex()
    {
        return complexTypesWithBlockStartIndex;
    }

    public void setComplexTypesWithBlockStartIndex(Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex)
    {
        this.complexTypesWithBlockStartIndex = complexTypesWithBlockStartIndex;
    }

    public List<InMemoryCube> getSlices()
    {
        return slices;
    }

    public KeyGenerator getKeyGenerator()
    {
        return keyGenerator;
    }

    public int getCurrentSliceIndex()
    {
        return currentSliceIndex;
    }

    public String getFactTableName()
    {
        return factTableName;
    }

    public QueryFilterInfo getInfo()
    {
        return info;
    }

    public void setSlices(List<InMemoryCube> slices)
    {
        this.slices = slices;
    }

    public void setKeyGenerator(KeyGenerator keyGenerator)
    {
        this.keyGenerator = keyGenerator;
    }

    public void setCurrentSliceIndex(int currentSliceIndex)
    {
        this.currentSliceIndex = currentSliceIndex;
    }

    public void setFactTableName(String factTableName)
    {
        this.factTableName = factTableName;
    }

    public void setInfo(QueryFilterInfo info)
    {
        this.info = info;
    }

    public double[] getNewDefaultValues()
    {
        return newDefaultValues;
    }

    public void setNewDefaultValues(double[] newDefaultValues)
    {
        this.newDefaultValues = newDefaultValues;
    }

    public String[] getNewDimension()
    {
        return newDimension;
    }

    public void setNewDimension(String[] newDimension)
    {
        this.newDimension = newDimension;
    }

    public String[] getNewMeasures()
    {
        return newMeasures;
    }

    public void setNewMeasures(String[] newMeasures)
    {
        this.newMeasures = newMeasures;
    }

    public int[] getNewDimensionSurrogates()
    {
        return newDimensionSurrogates;
    }

    public void setNewDimensionSurrogates(int[] newDimensionSurrogates)
    {
        this.newDimensionSurrogates = newDimensionSurrogates;
    }

    public String[] getNewDimensionDefaultValue()
    {
        return newDimensionDefaultValue;
    }

    public void setNewDimensionDefaultValue(String[] newDimensionDefaultValue)
    {
        this.newDimensionDefaultValue = newDimensionDefaultValue;
    }
    public void setHybridStoreModel(HybridStoreModel hybridStoreModel)
    {
        this.hybridStoreModel=hybridStoreModel;
        
    }
    
    public HybridStoreModel getHybridStoreModel()
    {
        return this.hybridStoreModel;
    }
}
