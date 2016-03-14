package com.huawei.unibi.molap.engine.schema.metadata;

import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.impl.QueryFilterInfo;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

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
}
