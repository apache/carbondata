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

import java.util.ArrayList;
import java.util.List;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : AggregateTable.java
 * Class Description : 
 * Version 1.0
 */
public class AggregateTable
{
    
    /**
     * aggregateTableName
     */
    private String aggregateTableName;
    
    /**
     * aggLevels
     */
    private String[] aggLevels;
    
    /**
     * aggLevels
     */
    private String[] aggLevelsActualName;
    
    /**
     * actualAggLevels
     */
    private String[] actualAggLevels;

    /**
     * aggMeasure
     */
    private String[] aggMeasure;
    
    /**
     * aggregator
     */
    private String[] aggregator;
    
    /**
     * Agg Namesssss
     */
    private String[] aggNames;
    
    /**
     * aggColuName
     */
    private String[] aggColuName;
    
    /**
     * aggregateClass
     */
    private String[] aggregateClass;

	//private String tableNameForAggr;
	
	private List<AggregateTable> dependentAggTables=new ArrayList<AggregateTable>(10);
    

	public List<AggregateTable> getDependentAggTables() {
		return dependentAggTables;
	}
	public void setDependentAggTables(List<AggregateTable> dependentAggTables) {
		this.dependentAggTables = dependentAggTables;
	}
	/**
     * @return
     */
    public String getAggregateTableName()
    {
        return aggregateTableName;
    }
    /**
     * @param aggregateTableName
     */
    public void setAggregateTableName(String aggregateTableName)
    {
        this.aggregateTableName = aggregateTableName;
    }
    /**
     * @return
     */
    public String[] getAggLevels()
    {
        return aggLevels;
    }
    /**
     * @param aggLevels
     */
    public void setAggLevels(String[] aggLevels)
    {
        this.aggLevels = aggLevels;
    }
    /**
     * @return
     */
    public String[] getAggMeasure()
    {
        return aggMeasure;
    }
    /**
     * @param aggMeasure
     */
    public void setAggMeasure(String[] aggMeasure)
    {
        this.aggMeasure = aggMeasure;
    }
    /**
     * @return
     */
    public String[] getAggregator()
    {
        return aggregator;
    }
    /**
     * @param aggregator
     */
    public void setAggregator(String[] aggregator)
    {
        this.aggregator = aggregator;
    }

    /**
     * 
     * @return Returns the actualAggLevels.
     * 
     */
    public String[] getActualAggLevels()
    {
        return actualAggLevels;
    }

    /**
     * 
     * @param actualAggLevels The actualAggLevels to set.
     * 
     */
    public void setActualAggLevels(String[] actualAggLevels)
    {
        this.actualAggLevels = actualAggLevels;
    }
    /**
     * 
     * @return Returns the aggNames.
     * 
     */
    public String[] getAggNames()
    {
        return aggNames;
    }
    /**
     * 
     * @param aggNames The aggNames to set.
     * 
     */
    public void setAggNames(String[] aggNames)
    {
        this.aggNames = aggNames;
    }
    /**
     * @return the aggregateClass
     */
    public String[] getAggregateClass()
    {
        return aggregateClass;
    }
    /**
     * @param aggregateClass the aggregateClass to set
     */
    public void setAggregateClass(String[] aggregateClass)
    {
        this.aggregateClass = aggregateClass;
    }
    /**
     * @return the aggLevelsActualName
     */
    public String[] getAggLevelsActualName()
    {
        return aggLevelsActualName;
    }
    /**
     * @param aggLevelsActualName the aggLevelsActualName to set
     */
    public void setAggLevelsActualName(String[] aggLevelsActualName)
    {
        this.aggLevelsActualName = aggLevelsActualName;
    }
	/**
	 * @return the aggColuName
	 */
	public String[] getAggColuName() {
		return aggColuName;
	}
	/**
	 * @param aggColuName the aggColuName to set
	 */
	public void setAggColuName(String[] aggColuName) {
		this.aggColuName = aggColuName;
	}
	/*public void setTableNameForAggregate(String tableNameForAggr) {
		this.tableNameForAggr=tableNameForAggr;
		
	}*/

}
