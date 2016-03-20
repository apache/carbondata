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

package com.huawei.unibi.molap.util;

import java.util.List;

import com.huawei.unibi.molap.olap.MolapDef.Cube;

public class MolapCubeInfo
{

    /**
     * cubeName.
     */
    private String cubeName;

    private String schemaName;

    private List<String> factAndAggtables;

    private Cube cube;
    
    private String schemaPath;
    
    

    /**
     * getCube
     * @return Cube
     */
    public Cube getCube()
    {
        return cube;
    }

    /**
     * setCube
     * @param cube void
     */
    public void setCube(Cube cube)
    {
        this.cube = cube;
    }

    /**
     * getCubeName
     * @return String
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * setCubeName
     * @param cubeName void
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * getSchemaName
     * @return String
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * setSchemaName
     * @param schemaName void
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * getTableNames
     * @return List<String>
     */
    public List<String> getTableNames()
    {
        return factAndAggtables;
    }

    /**
     * setTableNames
     * @param tableNames void
     */
    public void setTableNames(List<String> tableNames)
    {
        this.factAndAggtables = tableNames;
    }

    /**
     * setCubeName
     * @param cube void
     */
    public void setCubeName(Cube cube)
    {
        this.cube = cube;

    }

    /**
     * @return the schemaPath
     */
    public String getSchemaPath()
    {
        return schemaPath;
    }

    /**
     * @param schemaPath the schemaPath to set
     */
    public void setSchemaPath(String schemaPath)
    {
        this.schemaPath = schemaPath;
    }
}
