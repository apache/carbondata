/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.processing.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

/**
 * Wrapper Data Load Schema object which will be used to
 * support relation while data loading
 */
public class CarbonDataLoadSchema implements Serializable {

  /**
   * default serializer
   */
  private static final long serialVersionUID = 1L;

  /**
   * CarbonTable info
   */
  private CarbonTable carbonTable;

  /**
   * dimension table and relation info
   */
  private List<DimensionRelation> dimensionRelationList;

  /**
   * CarbonDataLoadSchema constructor which takes CarbonTable
   *
   * @param carbonTable
   */
  public CarbonDataLoadSchema(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
    this.dimensionRelationList = new ArrayList<DimensionRelation>();
  }

  /**
   * get dimension relation list
   *
   * @return dimensionRelationList
   */
  public List<DimensionRelation> getDimensionRelationList() {
    return dimensionRelationList;
  }

  /**
   * get carbontable
   *
   * @return carbonTable
   */
  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  /**
   * Dimension Relation object which will be filled from
   * Load DML Command to support normalized table data load
   */
  public static class DimensionRelation implements Serializable {
    /**
     * default serializer
     */
    private static final long serialVersionUID = 1L;

    /**
     * dimension tableName
     */
    private String tableName;

    /**
     * relation with fact and dimension table
     */
    private Relation relation;

    /**
     * Columns to selected from dimension table.
     * Hierarchy in-memory table should be prepared
     * based on selected columns
     */
    private List<String> columns;

    /**
     * constructor
     *
     * @param tableName       - dimension table name
     * @param relation        - fact foreign key with dimension primary key mapping
     * @param columns         - list of columns to be used from this dimension table
     */
    public DimensionRelation(String tableName, Relation relation,
        List<String> columns) {
      this.tableName = tableName;
      this.relation = relation;
      this.columns = columns;
    }

    /**
     * @return tableName
     */
    public String getTableName() {
      return tableName;
    }

    /**
     * @return relation
     */
    public Relation getRelation() {
      return relation;
    }

    /**
     * @return columns
     */
    public List<String> getColumns() {
      return columns;
    }
  }

  /**
   * Relation class to specify fact foreignkey column with
   * dimension primary key column
   */
  public static class Relation implements Serializable {
    /**
     * default serializer
     */
    private static final long serialVersionUID = 1L;

    /**
     * Fact foreign key column
     */
    private String factForeignKeyColumn;

    /**
     * dimension primary key column
     */
    private String dimensionPrimaryKeyColumn;

    /**
     * constructor
     *
     * @param factForeignKeyColumn      - Fact Table Foreign key
     * @param dimensionPrimaryKeyColumn - Dimension Table primary key
     */
    public Relation(String factForeignKeyColumn, String dimensionPrimaryKeyColumn) {
      this.factForeignKeyColumn = factForeignKeyColumn;
      this.dimensionPrimaryKeyColumn = dimensionPrimaryKeyColumn;
    }

    /**
     * @return factForeignKeyColumn
     */
    public String getFactForeignKeyColumn() {
      return factForeignKeyColumn;
    }

    /**
     * @return dimensionPrimaryKeyColumn
     */
    public String getDimensionPrimaryKeyColumn() {
      return dimensionPrimaryKeyColumn;
    }
  }
}
