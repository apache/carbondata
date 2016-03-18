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
package com.huawei.unibi.molap.datastorage.store.dataholder;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapReadDataHolder.java
 * Class Description : data holder which will be used by all the classes when they want to get the data 
 * Class Version 1.0
 */
public class MolapReadDataHolder
{

    /**
     * doubleValues
     */
    private double[] doubleValues;
    
    /**
     * byteValues
     */
    private byte[][] byteValues;
    
    /**
     * @return the doubleValues
     */
    public double[] getReadableDoubleValues()
    {
        return doubleValues;
    }

    /**
     * @return the byteValues
     */
    public byte[][] getReadableByteArrayValues()
    {
        return byteValues;
    }

    /**
     * @param doubleValues the doubleValues to set
     */
    public void setReadableDoubleValues(double[] doubleValues)
    {
        this.doubleValues = doubleValues;
    }

    /**
     * @param byteValues the byteValues to set
     */
    public void setReadableByteValues(byte[][] byteValues)
    {
        this.byteValues = byteValues;
    }
    
    /**
     * below method will be used to get the double value by index
     * @param index
     * @return double values
     */
    public double getReadableDoubleValueByIndex(int index)
    {
        return this.doubleValues[index];
    }
    
    /**
     * below method will be used to get the readable byte array value by index
     * @param index
     * @return byte array value
     */
    public byte[] getReadableByteArrayValueByIndex(int index)
    {
        return this.byteValues[index];
    }
}
