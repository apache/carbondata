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

package org.carbondata.query.datastorage;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.datastorage.streams.DataInputStream;

public class HierarchyStore {
    /**
     *
     */
    private String hierName;

    /**
     *
     */
    private String tableName;

    /**
     * Fact table Name
     */
    private String factTableName;

    /**
     * A null represents the hierarchy has only one level and the hierarchy can
     * be directly loaded from member cache.
     */
    // private Map dimension = new HashMap();

    private HierarchyBtreeStore btreeStore;

    /**
     *
     */
    private org.carbondata.core.olap.MolapDef.Hierarchy rolapHierarchy;

    /**
     *
     */
    private String dimeName;

    public HierarchyStore(org.carbondata.core.olap.MolapDef.Hierarchy rolapHierarchy,
            String factTableName, String dimensionName) {
        this.dimeName = dimensionName;
        this.hierName = rolapHierarchy.name == null ? dimensionName : rolapHierarchy.name;

        this.rolapHierarchy = rolapHierarchy;
        tableName = hierName.replaceAll(" ", "_") + ".hierarchy";
        this.factTableName = factTableName;
    }

    /**
     * Getter for hierarchy
     */
    public org.carbondata.core.olap.MolapDef.Hierarchy getRolapHierarchy() {
        return rolapHierarchy;
    }

    /**
     * Getter for dimension name in which this hierarchy present
     */
    public String getDimensionName() {
        return dimeName;
    }

    /**
     * @param keyGen
     * @param factStream
     */
    public void build(KeyGenerator keyGen, DataInputStream factStream) {
        btreeStore = new HierarchyBtreeStore(keyGen);
        btreeStore.build(factStream);
    }

    /**
     * @return the hierName
     */
    public String getHierName() {
        return hierName;
    }

    /**
     * @param hierName
     *            the hierName to set
     */
    /*public void setHierName(String hierName)
    {
        this.hierName = hierName;
    }*/

    /**
     * @return the dimension
     */
    // public Map getCache()
    // {
    // return dimension;
    // }

    /**
     * @param dimension
     *            the dimension to set
     */
    // public void setDimension(Map dimension)
    // {
    // this.dimension = dimension;
    // }

    //    public String getLevelName()
    //    {
    //        for(MolapDef.Level level : rolapHierarchy.levels)
    //        {
    //            if(!level.isAll())
    //            {
    //                return level.getName();
    //            }
    //        }
    //
    //        return null;
    //    }

    /**
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return
     */
    /*public HierarchyBtreeStore getHierBTreeStore()
    {
        return btreeStore;
    }*/

    /**
     * @return Returns the factTableName.
     */
    public String getFactTableName() {
        return factTableName;
    }

    // private Member dummy = new Member(0, "");

    // public boolean findMember(Long[] keys)
    // {
    // Map currentLevelCache = dimension;
    // for(int i=0; i< keys.length; i++)
    // {
    // //dummy.setDimension(keys[i]);
    // //currentLevelCache = (Map) currentLevelCache.get(dummy);
    // if(currentLevelCache == null)
    // {
    // return false;
    // }
    // }
    //
    // return true;
    // }
}
