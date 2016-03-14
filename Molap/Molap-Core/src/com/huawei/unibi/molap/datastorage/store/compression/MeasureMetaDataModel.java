/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */

package com.huawei.unibi.molap.datastorage.store.compression;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : FactReaderIterator.java
 * Class Description : Iterator class to iterate over leaf node and return the tuple
 * Class Version 1.0
 */
public class MeasureMetaDataModel
{
    /**
     * maxValue
     */
    private double[] maxValue;

    /**
     * minValue
     */
    private double[] minValue;

    /**
     * decimal
     */
    private int[] decimal;

    /**
     * measureCount
     */
    private int measureCount;

    /**
     * uniqueValue
     */
    private double[] uniqueValue;

    /**
     * type
     */
    private char[] type;

    /**
     * dataTypeSelected
     */
    private byte[] dataTypeSelected;
    
    private double[] minValueFactForAgg;
    
    public MeasureMetaDataModel()
    {
        
    }
    /**
     * MeasureMetaDataModel Constructor
     * @param minValue
     * @param maxValue
     * @param decimal
     * @param measureCount
     * @param uniqueValue
     * @param type
     */
    public MeasureMetaDataModel(double[] minValue, double[] maxValue, int[] decimal, int measureCount,
            double[] uniqueValue, char[] type,byte[] dataTypeSelected)
    {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.decimal = decimal;
        this.measureCount = measureCount;
        this.uniqueValue = uniqueValue;
        this.type=type;
        this.dataTypeSelected=dataTypeSelected;
    }

    /**
     * get Max value
     * 
     * @return
     */
    public double[] getMaxValue()
    {
        return maxValue;
    }

    /**
     * set max value
     * 
     * @param maxValue
     */
    public void setMaxValue(double[] maxValue)
    {
        this.maxValue = maxValue;
    }

    /**
     * 
     * getMinValue
     * 
     * @return
     */
    public double[] getMinValue()
    {
        return minValue;
    }

    /**
     * setMinValue
     * 
     * @param minValue
     */
    public void setMinValue(double[] minValue)
    {
        this.minValue = minValue;
    }

    /**
     * getDecimal
     * 
     * @return
     */
    public int[] getDecimal()
    {
        return decimal;
    }

    /**
     * setDecimal
     * 
     * @param decimal
     */
    public void setDecimal(int[] decimal)
    {
        this.decimal = decimal;
    }

    /**
     * getMeasureCount
     * 
     * @return
     */
    public int getMeasureCount()
    {
        return measureCount;
    }

    /**
     * setMeasureCount
     * 
     * @param measureCount
     */
    public void setMeasureCount(int measureCount)
    {
        this.measureCount = measureCount;
    }

    /**
     * getUniqueValue
     * 
     * @return
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }

    /**
     * setUniqueValue
     * 
     * @param uniqueValue
     */
    public void setUniqueValue(double[] uniqueValue)
    {
        this.uniqueValue = uniqueValue;
    }

    /**
     * @return the type
     */
    public char[] getType()
    {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(char[] type)
    {
        this.type = type;
    }

    /**
     * @return the dataTypeSelected
     */
    public byte[] getDataTypeSelected()
    {
        return dataTypeSelected;
    }

    /**
     * @param dataTypeSelected the dataTypeSelected to set
     */
    public void setDataTypeSelected(byte[] dataTypeSelected)
    {
        this.dataTypeSelected = dataTypeSelected;
    }
    /**
     * @return the minValueFactForAgg
     */
    public double[] getMinValueFactForAgg()
    {
        return minValueFactForAgg;
    }
    /**
     * @param minValueFactForAgg the minValueFactForAgg to set
     */
    public void setMinValueFactForAgg(double[] minValueFactForAgg)
    {
        this.minValueFactForAgg = minValueFactForAgg;
    }

}