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

package com.huawei.unibi.molap.schema.metadata;

import java.util.List;

import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.util.MolapSliceAndFiles;



public class MolapColumnarFactMergerInfo
{
    private String tableName;

    private int mdkeyLength;

    private int measureCount;

    private String schemaName;

    private String cubeName;

    private boolean isGroupByEnabled;

    private String[] aggregators;

    private String[] aggregatorClass;

    private int[] dimLens;
    
    private char[] type;
    
    private String destinationLocation;
    
    private List<MolapSliceAndFiles> slicesFromHDFS;
    
    private boolean isMergingRequestForCustomAgg;

	private boolean isUpdateFact;
	
	private KeyGenerator globalKeyGen;
    
    /**
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * @return the mdkeyLength
     */
    public int getMdkeyLength()
    {
        return mdkeyLength;
    }

    /**
     * @return the measureCount
     */
    public int getMeasureCount()
    {
        return measureCount;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * @return the isGroupByEnabled
     */
    public boolean isGroupByEnabled()
    {
        return isGroupByEnabled;
    }

    /**
     * @return the aggregators
     */
    public String[] getAggregators()
    {
        return aggregators;
    }

    /**
     * @return the aggregatorClass
     */
    public String[] getAggregatorClass()
    {
        return aggregatorClass;
    }

    /**
     * @return the dimLens
     */
    public int[] getDimLens()
    {
        return dimLens;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * @param mdkeyLength the mdkeyLength to set
     */
    public void setMdkeyLength(int mdkeyLength)
    {
        this.mdkeyLength = mdkeyLength;
    }

    /**
     * @param measureCount the measureCount to set
     */
    public void setMeasureCount(int measureCount)
    {
        this.measureCount = measureCount;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @param isGroupByEnabled the isGroupByEnabled to set
     */
    public void setGroupByEnabled(boolean isGroupByEnabled)
    {
        this.isGroupByEnabled = isGroupByEnabled;
    }

    /**
     * @param aggregators the aggregators to set
     */
    public void setAggregators(String[] aggregators)
    {
        this.aggregators = aggregators;
    }

    /**
     * @param aggregatorClass the aggregatorClass to set
     */
    public void setAggregatorClass(String[] aggregatorClass)
    {
        this.aggregatorClass = aggregatorClass;
    }

    /**
     * @param dimLens the dimLens to set
     */
    public void setDimLens(int[] dimLens)
    {
        this.dimLens = dimLens;
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
     * @return the destinationLocation
     */
    public String getDestinationLocation()
    {
        return destinationLocation;
    }

    /**
     * @param destinationLocation the destinationLocation to set
     */
    public void setDestinationLocation(String destinationLocation)
    {
        this.destinationLocation = destinationLocation;
    }

    /**
     * @return the slicesFromHDFS
     */
    public List<MolapSliceAndFiles> getSlicesFromHDFS()
    {
        return slicesFromHDFS;
    }

    /**
     * @param slicesFromHDFS the slicesFromHDFS to set
     */
    public void setSlicesFromHDFS(List<MolapSliceAndFiles> slicesFromHDFS)
    {
        this.slicesFromHDFS = slicesFromHDFS;
    }

    /**
     * @return the isMergingRequestForCustomAgg
     */
    public boolean isMergingRequestForCustomAgg()
    {
        return isMergingRequestForCustomAgg;
    }

    /**
     * @param isMergingRequestForCustomAgg the isMergingRequestForCustomAgg to set
     */
    public void setMergingRequestForCustomAgg(boolean isMergingRequestForCustomAgg)
    {
        this.isMergingRequestForCustomAgg = isMergingRequestForCustomAgg;
    }

	public void setIsUpdateFact(boolean isUpdateFact) {
		this.isUpdateFact=isUpdateFact;
		
	}
	
	public boolean isUpdateFact() {
		return isUpdateFact;
		
	}

    /**
     * @return the globalKeyGen
     */
    public KeyGenerator getGlobalKeyGen()
    {
        return globalKeyGen;
    }

    /**
     * @param globalKeyGen the globalKeyGen to set
     */
    public void setGlobalKeyGen(KeyGenerator globalKeyGen)
    {
        this.globalKeyGen = globalKeyGen;
    }
}
