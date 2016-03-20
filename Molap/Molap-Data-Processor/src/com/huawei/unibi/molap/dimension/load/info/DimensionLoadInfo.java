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

package com.huawei.unibi.molap.dimension.load.info;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.schema.metadata.HierarchiesInfo;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedDimSurrogateKeyGen;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedSeqGenMeta;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :14-Nov-2013 6:57:43 PM
 * FileName : DimensionLoadInfo.java
 * Class Description :
 * Version 1.0
 */
public class DimensionLoadInfo
{
    /**
     * Hierarchies Info
     */
    private List<HierarchiesInfo> hierVOlist;
    
    /**
     * Surrogate keyGen
     */
    private MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen;
    
    /**
     * modifiedDimesions
     */
    private String modifiedDimesions;
    
    /**
     * Map of Connection
     */
    private Map<String, Connection> cons = new HashMap<String, Connection>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * dimFileLocDir
     */
    private String dimFileLocDir;
    
    /**
     * MolapCSVBasedSeqGenMeta
     */
    private MolapCSVBasedSeqGenMeta meta;
    
    /**
     * dimTableNames
     */
    private String[] dimTableNames;
    
    /**
     * drivers
     */
    private Map<String, String> drivers;
    
    /**
     * keyGenerator
     */
    private Map<String,KeyGenerator> keyGeneratorMap;
    
    /**
     * Dimcardinality
     */
    private int[] dimCardinality;

    /**
     * 
     * @return Returns the hierVOlist.
     * 
     */
    public List<HierarchiesInfo> getHierVOlist()
    {
        return hierVOlist;
    }

    /**
     * 
     * @param hierVOlist The hierVOlist to set.
     * 
     */
    public void setHierVOlist(List<HierarchiesInfo> hierVOlist)
    {
        this.hierVOlist = hierVOlist;
    }

    /**
     * 
     * @return Returns the surrogateKeyGen.
     * 
     */
    public MolapCSVBasedDimSurrogateKeyGen getSurrogateKeyGen()
    {
        return surrogateKeyGen;
    }

    /**
     * 
     * @param surrogateKeyGen The surrogateKeyGen to set.
     * 
     */
    public void setSurrogateKeyGen(MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen)
    {
        this.surrogateKeyGen = surrogateKeyGen;
    }

    /**
     * 
     * @return Returns the modifiedDimesions.
     * 
     */
    public String getModifiedDimesions()
    {
        return modifiedDimesions;
    }

    /**
     * 
     * @param modifiedDimesions The modifiedDimesions to set.
     * 
     */
    public void setModifiedDimesions(String modifiedDimesions)
    {
        this.modifiedDimesions = modifiedDimesions;
    }

    /**
     * 
     * @return Returns the cons.
     * 
     */
    public Map<String, Connection> getCons()
    {
        return cons;
    }

    /**
     * 
     * @param cons The cons to set.
     * 
     */
    public void setCons(Map<String, Connection> cons)
    {
        this.cons = cons;
    }

    /**
     * 
     * @return Returns the meta.
     * 
     */
    public MolapCSVBasedSeqGenMeta getMeta()
    {
        return meta;
    }

    /**
     * 
     * @param meta The meta to set.
     * 
     */
    public void setMeta(MolapCSVBasedSeqGenMeta meta)
    {
        this.meta = meta;
    }

    /**
     * 
     * @return Returns the dimFileLocDir.
     * 
     */
    public String getDimFileLocDir()
    {
        return dimFileLocDir;
    }

    /**
     * 
     * @param dimFileLocDir The dimFileLocDir to set.
     * 
     */
    public void setDimFileLocDir(String dimFileLocDir)
    {
        this.dimFileLocDir = dimFileLocDir;
    }

    /**
     * 
     * @return Returns the dimTableNames.
     * 
     */
    public String[] getDimTableNames()
    {
        return dimTableNames;
    }

    /**
     * 
     * @param dimTableNames The dimTableNames to set.
     * 
     */
    public void setDimTableNames(String[] dimTableNames)
    {
        this.dimTableNames = dimTableNames;
    }

    /**
     * 
     * @return Returns the drivers.
     * 
     */
    public Map<String, String> getDrivers()
    {
        return drivers;
    }

    /**
     * 
     * @param drivers The drivers to set.
     * 
     */
    public void setDrivers(Map<String, String> drivers)
    {
        this.drivers = drivers;
    }

    /**
     * @return the dimCardinality
     */
    public int[] getDimCardinality()
    {
        return dimCardinality;
    }

    /**
     * @param dimCardinality the dimCardinality to set
     */
    public void setDimCardinality(int[] dimCardinality)
    {
        this.dimCardinality = dimCardinality;
    }

    /**
     * get key generator.
     * 
     * @return
     *
     */
    public Map<String, KeyGenerator> getKeyGenerator()
    {
        return keyGeneratorMap;
    }

    /**
     * Set key generator.
     * 
     * @param keyGenMap
     *
     */
    public void setKeyGeneratorMap(Map<String, KeyGenerator> keyGenMap)
    {
        this.keyGeneratorMap = keyGenMap;
    }
    
    
    
}

