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

package com.huawei.unibi.molap.query.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dataset
{
    public static final String DATA_SET_NAME = "DATA_SET_NAME";
    public static final String DP_EXTENSION = ".dpxanalyzer";
    public static final String DATA_SET_EXTENSION = ".csv";
    /**
     * SQL connection parameters
     */
    public static final String DB_CONNECTION_NAME = "DB_CONNECTION_NAME";
    public static final String DRIVER_CLASS = "DRIVER_CLASS";
    public static final String DB_URL = "DB_URL";
    public static final String DB_USER_NAME = "DB_USER_NAME";
    public static final String DB_PASSWORD = "DB_PASSWORD";

    /**
     * Type of the data set from the supported enumeration 
     */
    private DatasetType type;
    
    /**
     * Name 
     */
    private String name;
    
    /**
     * Columns represented in   
     */
    private List<Column> columns;
    
    private Map<String, Object> properties = new HashMap<String, Object>(16);
    
    public Dataset(DatasetType type, String title, List<Column> columns)
    {
        super();
        this.type = type;
        this.name = title;
        this.columns = columns;
    }
    
    public Object getProperty(String propertyName)
    {
        return properties.get(propertyName);
    }
    
    public void setProperty(String propertyName, Object value)
    {
        properties.put(propertyName, value);
    }

    public static enum DatasetType
    {
        LOCAL_FILE,
        REPORT_DATA_EXPORT,
        REPORT_LINK_EXPORT,
        DB_SQL;
    }
    
    public static class Column
    {
        private String name;
        public Column(String name, String type, String description)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
        
    }
    public DatasetType getType()
    {
        return type;
    }
    
    public void setType(DatasetType type)
    {
        this.type = type;
    }
    
    public String getTitle()
    {
        return name;
    }
    
    public void setTitle(String title)
    {
        this.name = title;
    }
    
    public List<Column> getColumns()
    {
        return columns;
    }
    
    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }
    
    @Override
    public String toString()
    {
        return "<DS: Name= "+ name +", Columns= " + columns.toString() + '>';
    }
}
