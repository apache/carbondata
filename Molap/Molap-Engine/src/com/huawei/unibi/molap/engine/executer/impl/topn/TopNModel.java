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
QO/t7E6DVIlgmdNVJ9x1GjBp+gp3syE6S84goT1oKN/8lDzrnsIzP0lLe1wTg1RCJGirVTVq
WHENQ4lAQNcCouavqvPbPIMyAZq1vDUibMyYrUX4Fs/oI+9sZfmBSmcK9sqCJA==*/
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
package com.huawei.unibi.molap.engine.executer.impl.topn;

import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.query.MolapQuery.AxisType;


/**
 * It is the model class for topN in MOLAP
 * @author R00900208
 *
 */
public class TopNModel
{

    /**
     * Count
     */
    private int count;
    /**
     * topNType
     */
    private MolapTopNType topNType;
    /**
     * dimIndex
     */
    private int dimIndex;
    /**
     * msrIndex
     */
    private int msrIndex;
    /**
     * dimension
     */
    private Dimension dimension;
    /**
     * measure
     */
    private Measure measure;
    
    /**
     * topNBytePos
     */
    private byte[] topNMaskedBytes;
    
    /**
     * bytePos
     */
    private byte[] topNGroupMaskedBytes;
    
    /**
     * countMsrIndex
     */
    private int countMsrIndex;
    
    /**
     * avgMsrIndex
     */
    private int avgMsrIndex;
    
    /**
     * topNBytePos
     */
    private int[] topNMaskedBytesPos;
    
    /**
     * bytePos
     */
    private int[] topNGroupMaskedBytesPos;
    
    /**
     * breakHierarchy
     */
    private boolean breakHierarchy;
    
    /**
     * breakHierarchy
     */
    private AxisType axisType;
    
    /**
     * Constructor that takes topn meta information.
     * @param count
     * @param topNType
     * @param dimension
     * @param measure
     */
    public TopNModel(int count, MolapTopNType topNType, Dimension dimension, Measure measure)
    {
        this.count = count;
        this.topNType = topNType;
        this.dimension = dimension;
        this.measure = measure;
    }



    /**
     * It is enum class for TopN type.
     * @author R00900208
     *
     */
    public enum MolapTopNType
    {
        /**
         * Top
         */
        TOP,
        /**
         * Bottom
         */
        BOTTOM;
    }



    /**
     * @return the count
     */
    public int getCount()
    {
        return count;
    }



    /**
     * @return the topNType
     */
    public MolapTopNType getTopNType()
    {
        return topNType;
    }



    /**
     * @return the dimension
     */
    public int getDimIndex()
    {
        return dimIndex;
    }



    /**
     * @return the msrIndex
     */
    public int getMsrIndex()
    {
        return msrIndex;
    }



    /**
     * @return the dimension
     */
    public Dimension getDimension()
    {
        return dimension;
    }



    /**
     * @return the measure
     */
    public Measure getMeasure()
    {
        return measure;
    }



    /**
     * @param dimIndex the dimIndex to set
     */
    public void setDimIndex(int dimIndex)
    {
        this.dimIndex = dimIndex;
    }



    /**
     * @param msrIndex the msrIndex to set
     */
    public void setMsrIndex(int msrIndex)
    {
        this.msrIndex = msrIndex;
    }



    /**
     * @return the topNMaskedBytes
     */
    public byte[] getTopNMaskedBytes()
    {
        return topNMaskedBytes;
    }



    /**
     * @param topNMaskedBytes the topNMaskedBytes to set
     */
    public void setTopNMaskedBytes(byte[] topNMaskedBytes)
    {
        this.topNMaskedBytes = topNMaskedBytes;
    }



    /**
     * @return the topNGroupMaskedBytes
     */
    public byte[] getTopNGroupMaskedBytes()
    {
        return topNGroupMaskedBytes;
    }



    /**
     * @param topNGroupMaskedBytes the topNGroupMaskedBytes to set
     */
    public void setTopNGroupMaskedBytes(byte[] topNGroupMaskedBytes)
    {
        this.topNGroupMaskedBytes = topNGroupMaskedBytes;
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



    /**
     * @return the avgMsrIndex
     */
    public int getAvgMsrIndex()
    {
        return avgMsrIndex;
    }



    /**
     * @param avgMsrIndex the avgMsrIndex to set
     */
    public void setAvgMsrIndex(int avgMsrIndex)
    {
        this.avgMsrIndex = avgMsrIndex;
    }



    /**
     * @return the topNMaskedBytesPos
     */
    public int[] getTopNMaskedBytesPos()
    {
        return topNMaskedBytesPos;
    }



    /**
     * @param topNMaskedBytesPos the topNMaskedBytesPos to set
     */
    public void setTopNMaskedBytesPos(int[] topNMaskedBytesPos)
    {
        this.topNMaskedBytesPos = topNMaskedBytesPos;
    }



    /**
     * @return the topNGroupMaskedBytesPos
     */
    public int[] getTopNGroupMaskedBytesPos()
    {
        return topNGroupMaskedBytesPos;
    }



    /**
     * @param topNGroupMaskedBytesPos the topNGroupMaskedBytesPos to set
     */
    public void setTopNGroupMaskedBytesPos(int[] topNGroupMaskedBytesPos)
    {
        this.topNGroupMaskedBytesPos = topNGroupMaskedBytesPos;
    }



    /**
     * @return the breakHierarchy
     */
    public boolean isBreakHierarchy()
    {
        return breakHierarchy;
    }



    /**
     * @param breakHierarchy the breakHierarchy to set
     */
    public void setBreakHierarchy(boolean breakHierarchy)
    {
        this.breakHierarchy = breakHierarchy;
    }



    /**
     * @param measure the measure to set
     */
    public void setMeasure(Measure measure)
    {
        this.measure = measure;
    }



    /**
     * @return the axisType
     */
    public AxisType getAxisType()
    {
        return axisType;
    }



    /**
     * @param axisType the axisType to set
     */
    public void setAxisType(AxisType axisType)
    {
        this.axisType = axisType;
    }
    
}
