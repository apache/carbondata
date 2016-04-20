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

package org.carbondata.core.carbon.metadata.schema.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

/**
 * Mapping class for Carbon actual table
 */
public class CarbonTable implements Serializable {

    /**
     * serialization id
     */
    private static final long serialVersionUID = 8696507171227156445L;

    /**
     * database name
     */
    private String databaseName;

    /**
     * TableName, Dimensions list
     */
    private Map<String, List<CarbonDimension>> tableDimensionsMap;

    /**
     * table measures list.
     */
    private Map<String, List<CarbonMeasure>> tableMeasuresMap;

    /**
     * TableName, Measures list.
     */
    private String factTableName;

    /**
     * tableUniqueName
     */
    private String tableUniqueName;

    /**
     * Aggregate tables name
     */
    private List<String> aggregateTablesName;

	/**
     * metadata file path (check if it is really required )
     */
    private String metaDataFilepath;

    /**
     * last updated time
     */
    private long tableLastUpdatedTime;

    public CarbonTable() {
        this.tableDimensionsMap = new HashMap<String, List<CarbonDimension>>();
        this.tableMeasuresMap = new HashMap<String, List<CarbonMeasure>>();
        this.aggregateTablesName = new ArrayList<String>();
    }

    /**
     * @param tableInfo
     */
    public void loadCarbonTable(TableInfo tableInfo) {
        this.tableLastUpdatedTime = tableInfo.getLastUpdatedTime();
        this.databaseName = tableInfo.getDatabaseName();
        this.tableUniqueName = tableInfo.getTableUniqueName();
        this.factTableName = tableInfo.getFactTable().getTableName();
        this.metaDataFilepath = tableInfo.getMetaDataFilepath();
        fillDimensionsAndMeasuresForTables(tableInfo.getFactTable());
        List<TableSchema> aggregateTableList = tableInfo.getAggregateTableList();
    		for (TableSchema aggTable : aggregateTableList) {
    			this.aggregateTablesName.add(aggTable.getTableName());
    			fillDimensionsAndMeasuresForTables(aggTable);
    		}
    }

    /**
     * Fill dimensions and measures for carbon table
     * @param tableSchema
     */
    private void fillDimensionsAndMeasuresForTables(TableSchema tableSchema) {
        List<CarbonDimension> dimensions = new ArrayList<CarbonDimension>();
        List<CarbonMeasure> measures = new ArrayList<CarbonMeasure>();
        this.tableDimensionsMap.put(tableSchema.getTableName(), dimensions);
        this.tableMeasuresMap.put(tableSchema.getTableName(), measures);
        int dimensionOrdinal = 0;
        int measureOrdinal = 0;
        List<ColumnSchema> listOfColumns = tableSchema.getListOfColumns();
        for (int i=0;i<listOfColumns.size();i++) {
          ColumnSchema columnSchema = listOfColumns.get(i);
            if (columnSchema.isDimensionColumn()) {
              if(columnSchema.getNumberOfChild() > 0)
              {
                CarbonDimension complexDimension = new CarbonDimension(columnSchema, dimensionOrdinal++);
                complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
                dimensions.add(complexDimension);
                dimensionOrdinal = readAllComplexTypeChildrens(dimensionOrdinal, columnSchema.getNumberOfChild(), listOfColumns, complexDimension);
                i = dimensionOrdinal - 1;
              }
              else
              {
                dimensions.add(new CarbonDimension(columnSchema, dimensionOrdinal++));
              }
            } else {
                measures.add(new CarbonMeasure(columnSchema, measureOrdinal++));
            }
        }
    }
    
    /**
     * Read all primitive/complex children and set it as list of child carbon dimension to parent dimension
     * @param dimensionOrdinal
     * @param childCount
     * @param listOfColumns
     * @param parentDimension
     * @return
     */
    private int readAllComplexTypeChildrens(int dimensionOrdinal, int childCount, List<ColumnSchema> listOfColumns, CarbonDimension parentDimension)
    {
      for(int i=0;i<childCount;i++)
      {
        ColumnSchema columnSchema = listOfColumns.get(dimensionOrdinal);
        if (columnSchema.isDimensionColumn()) {
          if(columnSchema.getNumberOfChild() > 0)
          {
            CarbonDimension complexDimension = new CarbonDimension(columnSchema, dimensionOrdinal++);
            complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
            parentDimension.getListOfChildDimensions().add(complexDimension);
            dimensionOrdinal = readAllComplexTypeChildrens(dimensionOrdinal, columnSchema.getNumberOfChild(), listOfColumns, complexDimension);
          }
          else
          {
            parentDimension.getListOfChildDimensions().add(new CarbonDimension(columnSchema, dimensionOrdinal++));
          }
        }
      }
      return dimensionOrdinal;
    }

    /**
     * @return the databaseName
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * @return the tabelName
     */
    public String getFactTableName() {
        return factTableName;
    }

    /**
     * @return the tableUniqueName
     */
    public String getTableUniqueName() {
        return tableUniqueName;
    }
    
    /**
     * @return the metaDataFilepath
     */
    public String getMetaDataFilepath() {
        return metaDataFilepath;
    }

    /**
     * @return list of aggregate TablesName
     */
    public List<String> getAggregateTablesName() {
    	return aggregateTablesName;
    }
    /**
     * @return the tableLastUpdatedTime
     */
    public long getTableLastUpdatedTime() {
        return tableLastUpdatedTime;
    }

    /**
     * to get the number of dimension present in the table
     *
     * @param tableName
     * @return number of dimension present the table
     */
    public int getNumberOfDimensions(String tableName) {
        return tableDimensionsMap.get(tableName).size();
    }

    /**
     * to get the number of measures present in the table
     *
     * @param tableName
     * @return number of measures present the table
     */
    public int getNumberOfMeasures(String tableName) {
        return tableMeasuresMap.get(tableName).size();
    }

    /**
     * to get the all dimension of a table
     *
     * @param tableName
     * @return all dimension of a table
     */
    public List<CarbonDimension> getDimensionByTableName(String tableName) {
        return tableDimensionsMap.get(tableName);
    }
    
    /**
     * to get the all measure of a table
     *
     * @param tableName
     * @return all measure of a table
     */
    public List<CarbonMeasure> getMeasureByTableName(String tableName) {
        return tableMeasuresMap.get(tableName);
    }

    /**
     * to get particular dimension from a table
     * 
     * @param tableName
     * @param columnName
     * @return
     */
    public CarbonDimension getDimensionByName(String tableName, String columnName) {
        List<CarbonDimension> dimensionList = tableDimensionsMap.get(tableName);
        for (CarbonDimension dims : dimensionList) {
            if (dims.getColName().equalsIgnoreCase(columnName)) ;
            {
                return dims;
            }
        }
        return null;
    }

    /**
     * to get particular measure from table
     * 
     * @param tableName
     * @param columnName
     * @return
     */
    public CarbonMeasure getMeasureByName(String tableName, String columnName) {
        List<CarbonMeasure> measureList = tableMeasuresMap.get(tableName);
        for (CarbonMeasure msr : measureList) {
            if (msr.getColName().equalsIgnoreCase(columnName)) ;
            {
                return msr;
            }
        }
        return null;
    }
    
    /**
     * gets all children dimension for complex type
     * 
     * @param dimName
     * @return
     */
    public List<CarbonDimension> getChildren(String dimName) {
        List<CarbonDimension> retList = new ArrayList<CarbonDimension>();
        for (List<CarbonDimension> list : tableDimensionsMap.values()) {
            for (CarbonDimension carbonDimension : list) {
                if (carbonDimension.getColName().equals(dimName) || 
                		carbonDimension.getColName().startsWith(dimName +".")) {
                    retList.add(carbonDimension);
                }
            }
        }
        return retList;
    }

	/**
	 * @param databaseName
	 */
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * @param factTableName
	 */
	public void setFactTableName(String factTableName) {
		this.factTableName = factTableName;
	}
    
    
}
