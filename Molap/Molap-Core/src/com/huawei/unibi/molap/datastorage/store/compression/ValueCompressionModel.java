/**
 *
 */
package com.huawei.unibi.molap.datastorage.store.compression;

import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.util.ValueCompressionUtil.COMPRESSION_TYPE;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

/**
 * @author x00228947
 * 
 */
public class ValueCompressionModel
{
    /**
     * COMPRESSION_TYPE[] variable.
     */
    private COMPRESSION_TYPE[] compType;
    
    /**
     *  DataType[]  variable.
     */
    private DataType[] changedDataType;
    /**
     * maxValue
     */
    private double[] maxValue;
    /**
     *minValue.
     */
    private double[] minValue;
    
    private double[] minValueFactForAgg;
    
    /**
     * uniqueValue
     */
    private double[] uniqueValue;
    /**
     * decimal.
     */
    private int[] decimal;
    
    /**
     * aggType
     */
    private char[] type;
    
    /**
     * dataTypeSelected
     */
    private byte[] dataTypeSelected;
    /**
     * unCompressValues.
     */
    private UnCompressValue[] unCompressValues;

    /**
     * @return the compType
     */
    public COMPRESSION_TYPE[] getCompType()
    {
        return compType;
    }

    /**
     * @param compType
     *            the compType to set
     */
    public void setCompType(COMPRESSION_TYPE[] compType)
    {
        this.compType = compType;
    }

    /**
     * @return the changedDataType
     */
    public DataType[] getChangedDataType()
    {
        return changedDataType;
    }

    /**
     * @param changedDataType
     *            the changedDataType to set
     */
    public void setChangedDataType(DataType[] changedDataType)
    {
        this.changedDataType = changedDataType;
    }

    /**
     * @return the maxValue
     */
    public double[] getMaxValue()
    {
        return maxValue;
    }

    /**
     * @param maxValue
     *            the maxValue to set
     */
    public void setMaxValue(double[] maxValue)
    {
        this.maxValue = maxValue;
    }

    /**
     * @return the decimal
     */
    public int[] getDecimal()
    {
        return decimal;
    }

    /**
     * @param decimal
     *            the decimal to set
     */
    public void setDecimal(int[] decimal)
    {
        this.decimal = decimal;
    }

    /**
     * getUnCompressValues().
     * @return the unCompressValues
     */
    public UnCompressValue[] getUnCompressValues()
    {
        return unCompressValues;
    }

    /**
     * @param unCompressValues
     *            the unCompressValues to set
     */
    public void setUnCompressValues(UnCompressValue[] unCompressValues)
    {
        this.unCompressValues = unCompressValues;
    }

    /**
     * getMinValue
     * @return
     */
    public double[] getMinValue()
    {
        return minValue;
    }

    /**
     * setMinValue.
     * @param minValue
     */
    public void setMinValue(double[] minValue)
    {
        this.minValue = minValue;
    }

    
    /**
     * @return the aggType
     */
    public char[] getType()
    {
        return type;
    }

    /**
     * @param aggType the aggType to set
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
     * getUniqueValue
     * @return
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }

    /**
     * setUniqueValue
     * @param uniqueValue
     */
    public void setUniqueValue(double[] uniqueValue)
    {
        this.uniqueValue = uniqueValue;
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
