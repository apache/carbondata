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

package com.huawei.unibi.molap.metadata;

/**
 * Project Name NSE V3R7C10 
 * Module Name : MOLAP
 * Author :A00903119
 * Created Date :5-Aug-2013
 * FileName : LevelNormalizedProps.java
 * Class Description : The class will hold
 * properties to define whether a level is
 * normalized or not.
 * Version 1.0
 */

public class LevelNormalizedProps
{
    /**
     * isLevelNormalized
     */
    private boolean isLevelNormalized;
    
    /**
     * is this level present in fact file
     */
    private boolean isDimInFact;
    
    /**
     * hierarchy Name
     */
    private String hierName;
    
    /**
     * dimension Name
     */
    private String dimName;
    
    /**
     * 
     */
    private boolean hasAll;
    
    /**
     * @return
     */
    public String getDimName()
    {
        return dimName;
    }

    /**
     * @param dimName
     */
    public void setDimName(String dimName)
    {
        this.dimName = dimName;
    }

    /**
     * @return
     */
    public String getHierName()
    {
        return hierName;
    }

    /**
     * @param hierName
     */
    public void setHierName(String hierName)
    {
        this.hierName = hierName;
    }

    /**
     * Constructor
     */
    public LevelNormalizedProps() {
        
    }

    /**
     * @return
     */
    public boolean isLevelNormalized()
    {
        return isLevelNormalized;
    }

    /**
     * @param isLevelNormalized
     */
    public void setLevelNormalized(boolean isLevelNormalized)
    {
        this.isLevelNormalized = isLevelNormalized;
    }

    /**
     * @return
     */
    public boolean isDimInFact()
    {
        return isDimInFact;
    }

    /**
     * @param dimInFact
     */
    public void setDimInFact(boolean isDimInFact)
    {
        this.isDimInFact = isDimInFact;
    }

    /**
     * @return the hasAll
     */
    public boolean isHasAll()
    {
        return hasAll;
    }

    /**
     * @param hasAll the hasAll to set
     */
    public void setHasAll(boolean hasAll)
    {
        this.hasAll = hasAll;
    } 

}
