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

package com.huawei.unibi.molap.engine.schema.metadata;

public class SliceUniqueValueInfo
{
    private double[] uniqueValue;
    
    private String[] cols;
    
    
    /**
     * getLength
     * @return int
     */
    public int getLength()
    {
        return uniqueValue.length;
    }

    /**
     * getUniqueValue
     * @return double[]
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }

    /**
     * setUniqueValue
     * @param uniqueValue1 void
     */
    public void setUniqueValue(double[] uniqueValue1)
    {
        this.uniqueValue = uniqueValue1;
    }

    public String[] getCols()
    {
        return cols;
    }

    public void setCols(String[] cols1)
    {
        this.cols = cols1;
    }


}
