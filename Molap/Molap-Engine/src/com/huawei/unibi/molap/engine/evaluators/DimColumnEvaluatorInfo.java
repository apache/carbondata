package com.huawei.unibi.molap.engine.evaluators;

import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;


public class DimColumnEvaluatorInfo
{
    /**
     * column index in file
     */
    private int columnIndex=-1;
    
    /**
     * need compressed data from file  
     */
    private boolean needCompressedData;
    
    /**
     * list of filter need to apply
     */
    private byte[][] filterValues;
    
    /**
     * slice
     */
    private List<InMemoryCube> slices;
    
    /**
     * currentSliceIndex
     */
    private int currentSliceIndex;
    
    /**
     * dims
     */
    private Dimension dims;

    private Dimension[] dimensions;
    
    /**
     * rowIndex
     */
    private int rowIndex=-1;

    private boolean isDimensionExistsInCurrentSilce=true;
    
    private int rsSurrogates;
    
    private String defaultValue;
    
    private Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex; 

    public Map<Integer, GenericQueryType> getComplexTypesWithBlockStartIndex()
    {
        return complexTypesWithBlockStartIndex;
    }

    public void setComplexTypesWithBlockStartIndex(Map<Integer, GenericQueryType> complexTypesWithBlockStartIndex)
    {
        this.complexTypesWithBlockStartIndex = complexTypesWithBlockStartIndex;
    }
    
    public Dimension[] getDimensions()
    {
        return dimensions;
    }

    public void setDimensions(Dimension[] dimensions)
    {
        this.dimensions = dimensions;
    }
    
    public int getColumnIndex()
    {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    public boolean isNeedCompressedData()
    {
        return needCompressedData;
    }

    public void setNeedCompressedData(boolean needCompressedData)
    {
        this.needCompressedData = needCompressedData;
    }

    public byte[][] getFilterValues()
    {
        return filterValues;
    }

    public void setFilterValues(final byte[][] filterValues)
    {
        this.filterValues = filterValues;
    }

    public int getRowIndex()
    {
        return rowIndex;
    }

    public void setRowIndex(int rowIndex)
    {
        this.rowIndex = rowIndex;
    }

    public Dimension getDims()
    {
        return dims;
    }

    public void setDims(Dimension dims)
    {
        this.dims = dims;
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

    public boolean isDimensionExistsInCurrentSilce()
    {
        return isDimensionExistsInCurrentSilce;
    }

    public void setDimensionExistsInCurrentSilce(boolean isDimensionExistsInCurrentSilce)
    {
        this.isDimensionExistsInCurrentSilce = isDimensionExistsInCurrentSilce;
    }

    public int getRsSurrogates()
    {
        return rsSurrogates;
    }

    public void setRsSurrogates(int rsSurrogates)
    {
        this.rsSurrogates = rsSurrogates;
    }

    public String getDefaultValue()
    {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue)
    {
        this.defaultValue = defaultValue;
    }
}
