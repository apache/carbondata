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

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
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
   * Absolute table identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;

  /**
   * TableName, Dimensions list
   */
  private Map<String, List<CarbonDimension>> tableDimensionsMap;

  /**
   * table measures list.
   */
  private Map<String, List<CarbonMeasure>> tableMeasuresMap;

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
    this.tableUniqueName = tableInfo.getTableUniqueName();
    this.metaDataFilepath = tableInfo.getMetaDataFilepath();
    //setting unique table identifier
    CarbonTableIdentifier carbontableIdentifier =
        new CarbonTableIdentifier(tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName(), tableInfo.getFactTable().getTableId());
    this.absoluteTableIdentifier =
        new AbsoluteTableIdentifier(tableInfo.getStorePath(), carbontableIdentifier);

    fillDimensionsAndMeasuresForTables(tableInfo.getFactTable());
    List<TableSchema> aggregateTableList = tableInfo.getAggregateTableList();
    for (TableSchema aggTable : aggregateTableList) {
      this.aggregateTablesName.add(aggTable.getTableName());
      fillDimensionsAndMeasuresForTables(aggTable);
    }
  }

  /**
   * Fill dimensions and measures for carbon table
   *
   * @param tableSchema
   */
  private void fillDimensionsAndMeasuresForTables(TableSchema tableSchema) {
    List<CarbonDimension> dimensions = new ArrayList<CarbonDimension>();
    List<CarbonMeasure> measures = new ArrayList<CarbonMeasure>();
    this.tableDimensionsMap.put(tableSchema.getTableName(), dimensions);
    this.tableMeasuresMap.put(tableSchema.getTableName(), measures);
    int dimensionOrdinal = 0;
    int measureOrdinal = 0;
    int keyOrdinal = 0;
    int columnGroupOrdinal = -1;
    int previousColumnGroupId = -1;
    List<ColumnSchema> listOfColumns = tableSchema.getListOfColumns();
    int complexTypeOrdinal = -1;
    for (int i = 0; i < listOfColumns.size(); i++) {
      ColumnSchema columnSchema = listOfColumns.get(i);
      if (columnSchema.isDimensionColumn()) {
        if (columnSchema.getNumberOfChild() > 0) {
          CarbonDimension complexDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1, ++complexTypeOrdinal);
          complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          dimensions.add(complexDimension);
          dimensionOrdinal =
              readAllComplexTypeChildrens(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  listOfColumns, complexDimension);
          i = dimensionOrdinal - 1;
          complexTypeOrdinal = assignComplexOrdinal(complexDimension, complexTypeOrdinal);
        } else {
          if (!columnSchema.getEncodingList().contains(Encoding.DICTIONARY)) {
            dimensions.add(new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1, -1));
          } else if (columnSchema.getEncodingList().contains(Encoding.DICTIONARY)
              && columnSchema.getColumnGroupId() == -1) {
            dimensions
                .add(new CarbonDimension(columnSchema, dimensionOrdinal++, keyOrdinal++, -1, -1));
          } else {
            columnGroupOrdinal =
                previousColumnGroupId == columnSchema.getColumnGroupId() ? ++columnGroupOrdinal : 0;
            previousColumnGroupId = columnSchema.getColumnGroupId();
            dimensions.add(new CarbonDimension(columnSchema, dimensionOrdinal++, keyOrdinal++,
                columnGroupOrdinal, -1));

          }
        }
      } else {
        measures.add(new CarbonMeasure(columnSchema, measureOrdinal++));
      }
    }
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   *
   * @param dimensionOrdinal
   * @param childCount
   * @param listOfColumns
   * @param parentDimension
   * @return
   */
  private int readAllComplexTypeChildrens(int dimensionOrdinal, int childCount,
      List<ColumnSchema> listOfColumns, CarbonDimension parentDimension) {
    for (int i = 0; i < childCount; i++) {
      ColumnSchema columnSchema = listOfColumns.get(dimensionOrdinal);
      if (columnSchema.isDimensionColumn()) {
        if (columnSchema.getNumberOfChild() > 0) {
          CarbonDimension complexDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1, -1);
          complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          parentDimension.getListOfChildDimensions().add(complexDimension);
          dimensionOrdinal =
              readAllComplexTypeChildrens(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  listOfColumns, complexDimension);
        } else {
          parentDimension.getListOfChildDimensions()
              .add(new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1, -1));
        }
      }
    }
    return dimensionOrdinal;
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   *
   * @param dimensionOrdinal
   * @param childCount
   * @param listOfColumns
   * @param parentDimension
   * @return
   */
  private int assignComplexOrdinal(CarbonDimension parentDimension, int complexDimensionOrdianl) {
    for (int i = 0; i < parentDimension.getNumberOfChild(); i++) {
      CarbonDimension dimension = parentDimension.getListOfChildDimensions().get(i);
      if (dimension.getNumberOfChild() > 0) {
        dimension.setComplexTypeOridnal(++complexDimensionOrdianl);
        complexDimensionOrdianl = assignComplexOrdinal(dimension, complexDimensionOrdianl);
      } else {
        parentDimension.getListOfChildDimensions().get(i)
            .setComplexTypeOridnal(++complexDimensionOrdianl);
      }
    }
    return complexDimensionOrdianl;
  }

  /**
   * @return the databaseName
   */
  public String getDatabaseName() {
    return absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName();
  }

  /**
   * @return the tabelName
   */
  public String getFactTableName() {
    return absoluteTableIdentifier.getCarbonTableIdentifier().getTableName();
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
   * @return storepath
   */
  public String getStorePath() {
    return absoluteTableIdentifier.getStorePath();
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
   * to get particular measure from a table
   *
   * @param tableName
   * @param columnName
   * @return
   */
  public CarbonMeasure getMeasureByName(String tableName, String columnName) {
    List<CarbonMeasure> measureList = tableMeasuresMap.get(tableName);
    for (CarbonMeasure measure : measureList) {
      if (measure.getColName().equalsIgnoreCase(columnName)) {
        return measure;
      }
    }
    return null;
  }

  /**
   * to get particular dimension from a table
   *
   * @param tableName
   * @param columnName
   * @return
   */
  public CarbonDimension getDimensionByName(String tableName, String columnName) {
    List<CarbonDimension> dimList = tableDimensionsMap.get(tableName);
    for (CarbonDimension dim : dimList) {
      if (dim.getColName().equalsIgnoreCase(columnName)) {
        return dim;
      }
    }
    return null;
  }

  /**
   * gets all children dimension for complex type
   *
   * @param dimName
   * @return list of child dimensions
   */
  public List<CarbonDimension> getChildren(String dimName) {
    for (List<CarbonDimension> list : tableDimensionsMap.values()) {
      List<CarbonDimension> childDims = getChildren(dimName, list);
      if (childDims != null) {
        return childDims;
      }
    }
    return null;
  }

  /**
   * returns level 2 or more child dimensions
   *
   * @param dimName
   * @param dimensions
   * @return list of child dimensions
   */
  public List<CarbonDimension> getChildren(String dimName, List<CarbonDimension> dimensions) {
    for (CarbonDimension carbonDimension : dimensions) {
      if (carbonDimension.getColName().equals(dimName)) {
        return carbonDimension.getListOfChildDimensions();
      } else if (null != carbonDimension.getListOfChildDimensions()
          && carbonDimension.getListOfChildDimensions().size() > 0) {
        List<CarbonDimension> childDims =
            getChildren(dimName, carbonDimension.getListOfChildDimensions());
        if (childDims != null) {
          return childDims;
        }
      }
    }
    return null;
  }

  /**
   * @return absolute table identifier
   */
  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  /**
   * @return carbon table identifier
   */
  public CarbonTableIdentifier getCarbonTableIdentifier() {
    return absoluteTableIdentifier.getCarbonTableIdentifier();
  }

  /**
   * gets partition count for this table
   * TODO: to be implemented while supporting partitioning
   */
  public int getPartitionCount() {
    return 1;
  }
}
