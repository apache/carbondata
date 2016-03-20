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

import java.util.Map;

/**
 * Project Name NSE V3R7C00 Module Name : MOLAP Author :C00900810 Created Date
 * :24-Jun-2013 FileName : HierarichiesInfo.java Class Description : Version 1.0
 */
public class HierarchiesInfo
{

    /**
     * hierarichieName
     */
    private String hierarichieName;
    
    /**
     * columnIndex
     */
    private int[] columnIndex;
    
    /**
     * columnNames
     */
    private String[] columnNames;
    
    /**
     * columnPropMap
     */
    private Map<String,String[]> columnPropMap;
    
    /**
     * loadToHierarichiTable
     */
    private boolean loadToHierarichiTable;
    
    /**
     * query
     */
    private String query;
    
    /**
     * Is Time Dimension
     */
    private boolean isTimeDimension;
    
    /**
     * levelTypeColumnMap
     */
    private Map<String, String> levelTypeColumnMap;
    
    /**
     * @return
     */
    public boolean isLoadToHierarichiTable()
    {
        return loadToHierarichiTable;
    }

    /**
     * @param loadToHierarichiTable
     */
    public void setLoadToHierarichiTable(boolean loadToHierarichiTable)
    {
        this.loadToHierarichiTable = loadToHierarichiTable;
    }

    /**
     * @return
     */
    public String getHierarichieName()
    {
        return hierarichieName;
    }

    /**
     * @param hierarichieName
     */
    public void setHierarichieName(String hierarichieName)
    {
        this.hierarichieName = hierarichieName;
    }

    /**
     * @return
     */
    public int[] getColumnIndex()
    {
        return columnIndex;
    }

    /**
     * @param columnIndex
     */
    public void setColumnIndex(int[] columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    /**
     * @return
     */
    public String[] getColumnNames()
    {
        return columnNames;
    }

    /**
     * @param columnNames
     */
    public void setColumnNames(String[] columnNames)
    {
        this.columnNames = columnNames;
    }

    /**
     * @return
     */
    public Map<String, String[]> getColumnPropMap()
    {
        return columnPropMap;
    }

    /**
     * @param columnPropMap
     */
    public void setColumnPropMap(Map<String, String[]> columnPropMap)
    {
        this.columnPropMap = columnPropMap;
    }

    /**
     * 
     * @return Returns the query.
     * 
     */
    public String getQuery()
    {
        return query;
    }

    /**
     * 
     * @param query The query to set.
     * 
     */
    public void setQuery(String query)
    {
        this.query = query;
    }

    /**
     * 
     * @return Returns the isTimeDimension.
     * 
     */
    public boolean isTimeDimension()
    {
        return isTimeDimension;
    }

    /**
     * 
     * @param isTimeDimension The isTimeDimension to set.
     * 
     */
    public void setTimeDimension(boolean isTimeDimension)
    {
        this.isTimeDimension = isTimeDimension;
    }

    /**
     * 
     * @return Returns the levelTypeColumnMap.
     * 
     */
    public Map<String, String> getLevelTypeColumnMap()
    {
        return levelTypeColumnMap;
    }

    /**
     * 
     * @param levelTypeColumnMap The levelTypeColumnMap to set.
     * 
     */
    public void setLevelTypeColumnMap(Map<String, String> levelTypeColumnMap)
    {
        this.levelTypeColumnMap = levelTypeColumnMap;
    }

}
