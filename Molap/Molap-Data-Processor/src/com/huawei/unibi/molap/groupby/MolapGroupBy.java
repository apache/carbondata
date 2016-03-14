/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnBxt7+I8sIO9Q5mnc8FuRDuYYT6u9cGD1VhClPm9WFnWiQ9zM4lBFMhfUVTYtLFCH+xp
w46a1h11ZY9iONdNtfodITnzS94mwQABAVcSLLHTWE/0gjWS6/o4LNzY0+BTQA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.groupby;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;

/**
 * 
 * Project Name NSE V3R7C00 Module Name : Molap Data Processor Author K00900841
 * Created Date :21-May-2013 6:42:29 PM FileName : MolapGroupBy.java Class
 * Description : This class will be used for merging the same key (Group By)
 * Version 1.0
 */
public class MolapGroupBy
{
    /**
     * key array index
     */
    private int keyIndex;

    /**
     * column index mapping
     */
    private int[] columnIndexMapping;

    /**
     * aggregate type
     */
    private String[] aggType;

    /**
     * previous row
     */
    private Object[] prvRow;

    /**
     * previous row key
     */
    private byte[] prvKey;

    /**
     * 
     * 
     * @param aggType
     *            agg type
     * @param rowMeasures
     *            row Measures name
     * @param actual
     *            Measures actual Measures
     * @param row
     *            row
     * 
     */
    public MolapGroupBy(String aggType, String rowMeasures, String actualMeasures, Object[] row)
    {
        this.aggType = aggType.split(";");
        this.keyIndex = row.length - 1;
        this.columnIndexMapping = new int[this.aggType.length];
        updateColumnIndexMapping(rowMeasures.split(";"), actualMeasures.split(";"));
        addNewRow(row);
    }

    private void addNewRow(Object[] row)
    {
        int index = 0;
        Object[] newRow = new Object[columnIndexMapping.length + 1];
        for(int i = 0;i < columnIndexMapping.length;i++)
        {
            if(this.aggType[i].equals(MolapCommonConstants.COUNT))
            {
                if(row[columnIndexMapping[i]] != null)
                {
                    newRow[index++] = 1D;
                }
                else
                {
                    newRow[index++] = 0D;
                }
            }
            else if(!this.aggType[i].equals(MolapCommonConstants.MAX)
                    && !this.aggType[i].equals(MolapCommonConstants.MIN))
            {
                if(null != row[columnIndexMapping[i]])
                {//CHECKSTYLE:OFF    Approval No:Approval-344
                    newRow[index++] = row[columnIndexMapping[i]];
                }//CHECKSTYLE:ON
                else
                {
                    newRow[index++] = 0D;
                }
            }
            else
            {//CHECKSTYLE:OFF    Approval No:Approval-345
                newRow[index++] = row[columnIndexMapping[i]];
            }//CHECKSTYLE:ON
        }
        prvKey = (byte[])row[this.keyIndex];
        newRow[index] = prvKey;
        prvRow = newRow;
    }

    /**
     * This method will be used to update the column index mapping array which
     * will be used for mapping actual column with row column
     * 
     * @param rowMeasureName
     *            row Measure Name
     * @param actualMeasures
     *            actual Measures
     * 
     */
    private void updateColumnIndexMapping(String[] rowMeasureName, String[] actualMeasures)
    {
        int index = 0;
        for(int i = 0;i < actualMeasures.length;i++)
        {
            for(int j = 0;j < rowMeasureName.length;j++)
            {
                if(actualMeasures[i].equals(rowMeasureName[j]))
                {
                    this.columnIndexMapping[index++] = j;
                    break;
                }
            }
        }
    }

    /**
     * This method will be used to add new row it will check if new row and
     * previous row key is same then it will merger the measure values, else it
     * return the previous row
     * 
     * @param row
     *            new row
     * @return previous row
     * 
     */
    public Object[] add(Object[] row)
    {
        if(MolapDataProcessorUtil.compare(prvKey, (byte[])row[this.keyIndex]) == 0)
        {
            updateMeasureValue(row);

        }
        else
        {
            Object[] lastRow = prvRow;
            addNewRow(row);
            return lastRow;
        }
        return null;
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     * 
     * @param row
     *            row
     * 
     */
    private void updateMeasureValue(Object[] row)
    {

        for(int i = 0;i < columnIndexMapping.length;i++)
        {
            aggregateValue(prvRow[i], row[columnIndexMapping[i]], aggType[i], i);
        }
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     * 
     * @param object1
     *            previous row measure value
     * @param object2
     *            new row measure value
     * 
     * @param aggType
     * 
     * @param idx
     *            measure index
     * 
     */
    private void aggregateValue(Object object1, Object object2, String aggType, int idx)
    {
        if(aggType.equals(MolapCommonConstants.MAX))
        {
            if(null == object1 && null != object2)
            {
                prvRow[idx] = object2;
            }
            else if(null != object1 && null == object2)
            {
                prvRow[idx] = object1;
            }
            else if(null != object1 && null != object2)
            {
                prvRow[idx] = (Double)object1 > (Double)object2 ? object1 : object2;
            }
        }
        else if(aggType.equals(MolapCommonConstants.MIN))
        {
            if(null == object1 && null != object2)
            {
                prvRow[idx] = object2;
            }
            else if(null != object1 && null == object2)
            {
                prvRow[idx] = object1;
            }
            else if(null != object1 && null != object2)
            {
                prvRow[idx] = (Double)object1 < (Double)object2 ? object1 : object2;
            }
        }
        else if(aggType.equals(MolapCommonConstants.COUNT))
        {
            if(null != object2)
            {
                double d = (Double)prvRow[idx];
                d++;
                prvRow[idx] = d;
            }
        }
        else
        {
            if(null == object1 && null != object2)
            {
                prvRow[idx] = object2;
            }
            else if(null != object1 && null == object2)
            {
                prvRow[idx] = object1;
            }
            else if(null != object1 && null != object2)
            {
                prvRow[idx] = (Double)object1 + (Double)object2;
            }
        }
    }

    /**
     * This method will be used to get the last row
     * 
     * @return last row
     * 
     */
    public Object[] getLastRow()
    {
        return prvRow;
    }
}
