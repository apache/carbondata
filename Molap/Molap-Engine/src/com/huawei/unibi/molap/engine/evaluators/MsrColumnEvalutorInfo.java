package com.huawei.unibi.molap.engine.evaluators;

public class MsrColumnEvalutorInfo
{
    private int columnIndex=-1;
    
    private int rowIndex=-1;
    
    private boolean isCustomMeasureValue;
    
    private double uniqueValue;
    
    private String aggregator;
    
    private boolean isMeasureExistsInCurrentSlice=true;
    
    private double defaultValue;

    public int getColumnIndex()
    {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    public int getRowIndex()
    {
        return rowIndex;
    }

    public void setRowIndex(int rowIndex)
    {
        this.rowIndex = rowIndex;
    }

    public boolean isCustomMeasureValue()
    {
        return isCustomMeasureValue;
    }

    public void setCustomMeasureValue(boolean isCustomMeasureValue)
    {
        this.isCustomMeasureValue = isCustomMeasureValue;
    }

    public double getUniqueValue()
    {
        return uniqueValue;
    }

    public void setUniqueValue(double uniqueValue)
    {
        this.uniqueValue = uniqueValue;
    }

    /**
     * 
     * @return Returns the aggregator.
     * 
     */
    public String getAggregator()
    {
        return aggregator;
    }

    /**
     * 
     * @param aggregator The aggregator to set.
     * 
     */
    public void setAggregator(String aggregator)
    {
        this.aggregator = aggregator;
    }

    public boolean isMeasureExistsInCurrentSlice()
    {
        return isMeasureExistsInCurrentSlice;
    }

    public void setMeasureExistsInCurrentSlice(boolean isMeasureExistsInCurrentSlice)
    {
        this.isMeasureExistsInCurrentSlice = isMeasureExistsInCurrentSlice;
    }

    public double getDefaultValue()
    {
        return defaultValue;
    }

    public void setDefaultValue(double defaultValue)
    {
        this.defaultValue = defaultValue;
    }
}
