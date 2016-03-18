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
QO/t7MsXbQVFTBqptcthLGWwb91vBjOWveWJiLYVK9hZa0tSjcB/SjuabO1VNpZo/IP+0wbR
DYYpO9QsWPZA9c69yV8Gp8y04uzC+kEY2bimZVcEFvJMpchGg+3vpsaKogePog==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.schema.metadata;

import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.query.MolapQuery.AxisType;

/**
 * MeasureFilterProcessorModel
 * @author R00900208
 *
 */
public class MeasureFilterProcessorModel
{

    /**
     * dimIndex
     */
    private int dimIndex;
    
    /**
     * topNBytePos
     */
    private byte[] maskedBytes;
    
    /**
     * topNBytePos
     */
    private int[] maskedBytesPos;
    
    /**
     * dimension
     */
    private Dimension dimension;
    
    /**
     * AxisType
     */
    private AxisType axisType; 

    /**
     * @return the dimIndex
     */
    public int getDimIndex()
    {
        return dimIndex;
    }

    /**
     * @param dimIndex the dimIndex to set
     */
    public void setDimIndex(int dimIndex)
    {
        this.dimIndex = dimIndex;
    }

    /**
     * @return the maskedBytes
     */
    public byte[] getMaskedBytes()
    {
        return maskedBytes;
    }

    /**
     * @param maskedBytes the maskedBytes to set
     */
    public void setMaskedBytes(byte[] maskedBytes)
    {
        this.maskedBytes = maskedBytes;
    }

    /**
     * @return the maskedBytesPos
     */
    public int[] getMaskedBytesPos()
    {
        return maskedBytesPos;
    }

    /**
     * @param maskedBytesPos the maskedBytesPos to set
     */
    public void setMaskedBytesPos(int[] maskedBytesPos)
    {
        this.maskedBytesPos = maskedBytesPos;
    }

    /**
     * @return the dimension
     */
    public Dimension getDimension()
    {
        return dimension;
    }

    /**
     * @param dimension the dimension to set
     */
    public void setDimension(Dimension dimension)
    {
        this.dimension = dimension;
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
