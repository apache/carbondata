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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7N/eqFlMzXYmusttKjtTK8HVzUf/Ez3GLOxdBaC/zFpeYdA/iCel2Vq5AtyzYvG8JwUU
Qh23lMYHzmX0OEEvJL9Zh8v7onMLDG7/ULt+8bMg8GgeRvyQTKLt1UI83gWqXQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.executer.groupby;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.CalculatedMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CalculatedMeasureAggregatorImpl;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * This class aggregates and holds the rows as per the topN applied on dimension
 * and measure.
 * 
 * @author R00900208
 * 
 */
public class GroupByHolder
{

    private byte[] maskedBytes;

    /**
     * msrIndex
     */
//    private int msrIndex;

    /**
     * rows list
     */
    private List<byte[]> rows = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);

    /**
     * rows list
     */
    private List<MeasureAggregator[]> msrs = new ArrayList<MeasureAggregator[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);

    /**
     * lastRow
     */
    public byte[] lastRow;

//    /**
//     * MeasureAggregator
//     */
//    private MeasureAggregator agg;

    /**
     * MeasureAggregator
     */
    private MeasureAggregator[] aggs;

    /**
     * countMsrIndex
     */
//    private int countMsrIndex;

    /**
     * avgMsrIndex
     */
//    private int avgMsrIndex;

    /**
     * isCalcMsr
     */
    private boolean isCalcMsr;

    /**
     * Calc function
     */
    private MolapCalcFunction calcFunction;

    /**
     * maskedBytePos
     */
    private int[] maskedBytePos;

    /**
     * Constructor that takes dimension index and measure index on which topN
     * needs to be applied.
     * 
     * @param dimIndex
     * @param msrIndex
     * @param aggName
     * @param countMsrIndex
     * @param cubeUniqueName 
     * @param keyGenerator 
     */
    public GroupByHolder(byte[] maskedBytes, int msrIndex, String aggName, int countMsrIndex, int avgMsrIndex,
            boolean isCalcMsr, Measure[] queryMsrs, MolapCalcFunction calcFunction, int[] maskedBytePos, KeyGenerator keyGenerator, String cubeUniqueName)
    {
        this.maskedBytes = maskedBytes;
//        this.msrIndex = msrIndex;
//        this.countMsrIndex = countMsrIndex;
//        this.avgMsrIndex = avgMsrIndex;
        this.isCalcMsr = isCalcMsr;
        this.calcFunction = calcFunction;
        this.maskedBytePos = maskedBytePos;
//        aggs = AggUtil.getAggregators(queryMsrs, false, keyGenerator, cubeUniqueName);

//        if(avgMsrIndex >= 0)
//        {
//            aggs = AggUtil.getAggregators(queryMsrs, true, null, null);
//        }
        // if(isCalcMsr)
        // {
        // }
        // else
        // {
        // if(avgMsrIndex >= 0)
        // {
        // agg = AggUtil.getAggregator(MolapCommonConstants.AVERAGE, false,
        // null, null);
        // }
        // else
        // {
        // agg = AggUtil.getAggregator(aggName, false, null, null);
        // }
        // if(agg == null && aggName == null)
        // {
        // agg = AggUtil.getAggregator(MolapCommonConstants.SUM, false, null,
        // null);
        // }
        // }
    }

    /**
     * Add row to this holder.
     * 
     * @param row
     * @return, it returns true if it aggregated and belonged to same group.
     */
    public boolean addRow(byte[] row, MeasureAggregator[] aggregators)
    {
        if(lastRow == null)
        {
            rows.add(row);
            msrs.add(aggregators);
            lastRow = row;
//            aggregateData(row, aggregators);
            return true;
        }

        if(objectEquals(lastRow, row))
        {
            rows.add(row);
            msrs.add(aggregators);
            lastRow = row;
//            aggregateData(row, aggregators);
            return true;
        }
        return false;
    }

    /**
     * Aggregate the data
     * 
     * @param row
     */
   /* private void aggregateData(byte[] row, MeasureAggregator[] aggregators)
    {
        for(int i = 0;i < aggs.length;i++)
        {
            aggs[i].merge(aggregators[i]);
        }
//        if(avgMsrIndex >= 0)
//        {
//            agg.agg(aggregators[msrIndex].getValue(), aggregators[countMsrIndex].getValue());
//        }
    }*/

    /**
     * Return the value
     * 
     * @return
     * 
     */
    public double getValue()
    {
        if(isCalcMsr)
        {
            return getCalculatedMeasureValue();
        }
//        if(avgMsrIndex >= 0)
//        {
//            return agg.getValue();
//        }
        return Double.MIN_NORMAL;
    }

    /**
     * Get the average calculated value;
     * @return
     */
    public MeasureAggregator[] getMeasureAggregators()
    {
        return aggs;
    }
    
    /**
     * Get the calculated measure value.
     * @return
     */
    public double getCalculatedMeasureValue()
    {
        CalculatedMeasureAggregator aggregator = new CalculatedMeasureAggregatorImpl(calcFunction);
        aggregator.calculateCalcMeasure(aggs);
        return aggregator.getValue();
    }
    
    /**
     * Equals the array
     * 
     * @param lastRow
     * @param row
     * @return
     */
    private boolean objectEquals(byte[] lastRow, byte[] row)
    {
        if(null == maskedBytePos)
        {
            return false;
        }
        for(int i = 0;i < maskedBytePos.length;i++)
        {
            int lb = (maskedBytes[i] & lastRow[maskedBytePos[i]]);
            int rb = (maskedBytes[i] & row[maskedBytePos[i]]);
            if(lb != rb)
            {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the rows
     */
    public List<byte[]> getRows()
    {
        return rows;
    }

    /**
     * @return the msrs
     */
    public List<MeasureAggregator[]> getMsrs()
    {
        return msrs;
    }

}
