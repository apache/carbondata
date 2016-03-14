/**
 * ====================Copyright Notice =======================
 *  This file contains proprietary information of Huawei Technologies Co. Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2013
 * =========================================================
 * 
 */
package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.groupby.exception.MolapGroupByException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;

/**
 * 
 * Project Name V3R008
 * Module Name : 
 * Author s71955
 * Created Date :14-Oct-2014 10:32:00 AM
 * FileName : MolapSortKeyAggregator.java
 * Class Description :
 * Version 1.0
 * ====================Copyright Notice =======================
 *  This file contains proprietary information of Huawei Technologies Co. Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2010
 * =========================================================
 */
public class MolapSortKeyAggregator
{

    private int keyIndex;

    private String[] aggType;

    private String[] aggClassName;

    private boolean isFirst = true;

    private byte[] prvKey;

    private KeyGenerator factKeyGenerator;

    private List<Object[]> result = new ArrayList<Object[]>(20);

    /**
     * max value for each measure
     */
    private char[] type;

    /**
     * aggregators
     */
    private MeasureAggregator[] aggregators;

    /**
     * isNotNullValue
     */
    private boolean[] isNotNullValue;

    private int resultSize;
    
    private double[] mergedMinValue;
    
    

    /**
     * constructer.
     * @param aggType
     * @param aggClassName
     * @param factKeyGenerator
     * @param type
     */
    public MolapSortKeyAggregator(String[] aggType, String[] aggClassName,
            KeyGenerator factKeyGenerator, char[] type, double[] mergedMinValue)
    {
        this.keyIndex = aggType.length;
        this.aggType = aggType;
        this.aggClassName = aggClassName;
        this.factKeyGenerator = factKeyGenerator;
        resultSize = aggType.length + 1;
        this.type=type;
        this.mergedMinValue=mergedMinValue;
    }

    /**
     * getAggregatedData.
     * @Author s71955
     * @Description : getAggregatedData
     * @param rows
     * @return
     * @throws MolapGroupByException
     */
    public Object[][] getAggregatedData(Object[][] rows)
            throws MolapGroupByException
    {
        for(int i = 0;i < rows.length;i++)
        {
            add(rows[i]);
        }

        result.add(prepareResult());
        return result.toArray(new Object[result.size()][]);
    }

    /**
     * This method will be used to add new row it will check if new row and
     * previous row key is same then it will merger the measure values, else it
     * return the previous row
     * 
     * @param row
     *            new row
     * @return previous row
     * @throws MolapGroupByException
     * 
     */
    private void add(Object[] row) throws MolapGroupByException
    {
        if(isFirst)
        {

            isFirst = false;
            initialiseAggegators();
            addNewRow(row);

            return;
        }
        if(MolapDataProcessorUtil.compare(prvKey, (byte[])row[this.keyIndex]) == 0)
        {
            updateMeasureValue(row);
        }
        else
        {
            result.add(prepareResult());
            initialiseAggegators();
            addNewRow(row);
        }
    }

    private Object[] prepareResult()
    {
        Object[] out = new Object[resultSize];
        for(int i = 0;i < aggregators.length;i++)
        {
            if(type[i] != 'c')
            {
                if(isNotNullValue[i])
                {
                    out[i] = aggregators[i].getValue();

                }
                else
                {
                    out[i] = null;
                }
            }
            else
            {
                out[i] = aggregators[i].getByteArray();
            }
        }

        out[out.length - 1] = prvKey;
        return out;
    }

    private void initialiseAggegators()
    {
        aggregators = AggUtil.getAggregators(Arrays.asList(this.aggType),
                Arrays.asList(this.aggClassName), false, factKeyGenerator,
                null,mergedMinValue);
        isNotNullValue = new boolean[this.aggType.length];
        for(int i = 0;i < aggType.length;i++)
        {
            if(aggType[i].equals(MolapCommonConstants.DISTINCT_COUNT))
            {
                isNotNullValue[i] = true;
            }

        }
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
        for(int i = 0;i < aggregators.length;i++)
        {
            if(null != row[i])
            {
                double value = (Double)row[i];
                aggregators[i].agg(value, (byte[])row[row.length - 1], 0,
                        ((byte[])row[row.length - 1]).length);
            }
        }

    }

    /**
     * Below method will be used to add new row
     * 
     * @param row
     * 
     */
    private void addNewRow(Object[] row)
    {
        for(int i = 0;i < aggregators.length;i++)
        {
            if(null != row[i])
            {
                this.isNotNullValue[i] = true;
                double value = (Double)row[i];
                aggregators[i].agg(value, (byte[])row[row.length - 1], 0,
                        ((byte[])row[row.length - 1]).length);
            }
        }
        prvKey = (byte[])row[this.keyIndex];

    }

}
