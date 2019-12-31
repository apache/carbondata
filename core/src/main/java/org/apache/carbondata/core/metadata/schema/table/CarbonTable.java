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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.constants.SortScopeOptions;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonImplicitDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Mapping class for Carbon actual table
 */
public class CarbonTable implements Serializable, Writable {

  private static final Logger LOGGER = LogServiceFactory.getLogService(CarbonTable.class.getName());
  private static final long serialVersionUID = 8696507171227156445L;

  // The main object that contains all carbon table information, including
  // schema, store path, table properties, datamap related info, etc.
  // All other fields in CarbonTable can be derived from TableInfo.
  private TableInfo tableInfo;

  // Visible dimension columns that exposed to user (can be queried)
  private List<CarbonDimension> visibleDimensions;

  // All dimension columns including visible columns and implicit columns
  private List<CarbonDimension> allDimensions;

  // An ordered list, same order as when creating this table by user
  private List<CarbonColumn> createOrderColumn;

  // Implicit columns that for internal usage, like positionid and tupleid for update/delete
  // operation. see CARBON_IMPLICIT_COLUMN_POSITIONID, CARBON_IMPLICIT_COLUMN_TUPLEID
  private List<CarbonDimension> implicitDimensions;

  // Visible measure columns that exposed to user (can be queried)
  private List<CarbonMeasure> visibleMeasures;

  // All measure columns including visible columns and implicit columns
  private List<CarbonMeasure> allMeasures;

  /**
   * list of column drift
   */
  private List<CarbonDimension> columnDrift;

  // Bucket information defined by user when creating table
  // Will be deleted after 2.0
  @Deprecated
  private BucketingInfo bucket;

  // Partition (Range/List/Hash). This is not for Hive partition.
  // Will be deleted after 2.0
  @Deprecated
  private PartitionInfo partition;

  // Number of columns in SORT_COLUMNS table property
  private int numberOfSortColumns;

  // Number of no dictionary columns in SORT_COLUMNS
  private int numberOfNoDictSortColumns;

  // The last index of the dimension column in all columns
  private int dimensionOrdinalMax;

  // True if this table has datamap
  private boolean hasDataMapSchema;

  // True if local dictionary is enabled for this table
  private boolean isLocalDictionaryEnabled;

  // Cardinality threshold for local dictionary, below which dictionary will be generated
  private int localDictionaryThreshold;

  public CarbonTable() {
    this.visibleDimensions = new LinkedList<>();
    this.implicitDimensions = new LinkedList<>();
    this.visibleMeasures = new LinkedList<>();
    this.createOrderColumn = new LinkedList<>();
    this.columnDrift = new LinkedList<>();
  }

  /**
   * During creation of TableInfo from hivemetastore the DataMapSchemas and the columns
   * DataTypes are not converted to the appropriate child classes.
   * This method will cast the same to the appropriate classes
   */
  private static void updateTableInfo(TableInfo tableInfo) {
    List<DataMapSchema> dataMapSchemas = new ArrayList<>();
    for (DataMapSchema dataMapSchema : tableInfo.getDataMapSchemaList()) {
      DataMapSchema newDataMapSchema = DataMapSchemaFactory.INSTANCE
          .getDataMapSchema(dataMapSchema.getDataMapName(), dataMapSchema.getProviderName());
      newDataMapSchema.setChildSchema(dataMapSchema.getChildSchema());
      newDataMapSchema.setProperties(dataMapSchema.getProperties());
      newDataMapSchema.setRelationIdentifier(dataMapSchema.getRelationIdentifier());
      dataMapSchemas.add(newDataMapSchema);
    }
    tableInfo.setDataMapSchemaList(dataMapSchemas);
    for (ColumnSchema columnSchema : tableInfo.getFactTable().getListOfColumns()) {
      columnSchema.setDataType(DataTypeUtil
          .valueOf(columnSchema.getDataType(), columnSchema.getPrecision(),
              columnSchema.getScale()));
    }
    List<DataMapSchema> childSchema = tableInfo.getDataMapSchemaList();
    for (DataMapSchema dataMapSchema : childSchema) {
      if (dataMapSchema.childSchema != null
          && dataMapSchema.childSchema.getListOfColumns().size() > 0) {
        for (ColumnSchema columnSchema : dataMapSchema.childSchema.getListOfColumns()) {
          columnSchema.setDataType(DataTypeUtil
              .valueOf(columnSchema.getDataType(), columnSchema.getPrecision(),
                  columnSchema.getScale()));
        }
      }
    }
    if (tableInfo.getFactTable().getBucketingInfo() != null) {
      for (ColumnSchema columnSchema : tableInfo.getFactTable().getBucketingInfo()
          .getListOfColumns()) {
        columnSchema.setDataType(DataTypeUtil
            .valueOf(columnSchema.getDataType(), columnSchema.getPrecision(),
                columnSchema.getScale()));
      }
    }
    if (tableInfo.getFactTable().getPartitionInfo() != null) {
      for (ColumnSchema columnSchema : tableInfo.getFactTable().getPartitionInfo()
          .getColumnSchemaList()) {
        columnSchema.setDataType(DataTypeUtil
            .valueOf(columnSchema.getDataType(), columnSchema.getPrecision(),
                columnSchema.getScale()));
      }
    }
  }

  public static CarbonTable buildTable(String tablePath, String tableName,
      Configuration configuration) throws IOException {
    TableInfo tableInfoInfer = CarbonUtil.buildDummyTableInfo(tablePath, tableName, "null");
    // InferSchema from data file
    org.apache.carbondata.format.TableInfo tableInfo =
        CarbonUtil.inferSchema(tablePath, tableName, false, configuration);
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    for (org.apache.carbondata.format.ColumnSchema thriftColumnSchema : tableInfo.getFact_table()
        .getTable_columns()) {
      ColumnSchema columnSchema = thriftColumnSchemaToWrapperColumnSchema(thriftColumnSchema);
      if (columnSchema.getColumnReferenceId() == null) {
        columnSchema.setColumnReferenceId(columnSchema.getColumnUniqueId());
      }
      columnSchemaList.add(columnSchema);
    }
    tableInfoInfer.getFactTable().setListOfColumns(columnSchemaList);
    return CarbonTable.buildFromTableInfo(tableInfoInfer);
  }

  public static CarbonTable buildFromTablePath(String tableName, String dbName, String tablePath,
      String tableId) throws IOException {
    return SchemaReader.readCarbonTableFromStore(
        AbsoluteTableIdentifier.from(tablePath, dbName, tableName, tableId));
  }

  /**
   * Build {@link CarbonTable} from a {@link TableInfo} object.
   */
  public static CarbonTable buildFromTableInfo(TableInfo tableInfo) {
    CarbonTable table = new CarbonTable();
    updateTableByTableInfo(table, tableInfo);
    return table;
  }

  /**
   * Return table unique name
   */
  public static String buildUniqueName(String databaseName, String tableName) {
    return (databaseName + CarbonCommonConstants.UNDERSCORE + tableName).toLowerCase(
        Locale.getDefault());
  }

  /**
   * Get Dimension for columnName from list of dimensions
   */
  public static CarbonDimension getCarbonDimension(String columnName,
      List<CarbonDimension> dimensions) {
    CarbonDimension carbonDimension = null;
    for (CarbonDimension dim : dimensions) {
      if (dim.getColName().equalsIgnoreCase(columnName)) {
        carbonDimension = dim;
        break;
      }
    }
    return carbonDimension;
  }

  /**
   * Resolve the filter expression.
   */
  public static FilterResolverIntf resolveFilter(Expression filterExpression,
      AbsoluteTableIdentifier identifier) {
    try {
      FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
      return filterExpressionProcessor.getFilterResolver(filterExpression, identifier);
    } catch (Exception e) {
      throw new RuntimeException("Error while resolving filter expression", e);
    }
  }

  /**
   * Create a {@link CarbonTableBuilder} to create {@link CarbonTable}
   */
  public static CarbonTableBuilder builder() {
    return new CarbonTableBuilder();
  }

  /**
   * Update the carbon table by using the passed tableInfo
   */
  public static void updateTableByTableInfo(CarbonTable table, TableInfo tableInfo) {
    updateTableInfo(tableInfo);
    table.tableInfo = tableInfo;
    table.setTransactionalTable(tableInfo.isTransactionalTable());
    table.fillDimensionsAndMeasuresForTables(tableInfo.getFactTable());
    table.fillCreateOrderColumn();
    if (tableInfo.getFactTable().getBucketingInfo() != null) {
      table.bucket = tableInfo.getFactTable().getBucketingInfo();
    }
    if (tableInfo.getFactTable().getPartitionInfo() != null) {
      table.partition = tableInfo.getFactTable().getPartitionInfo();
    }
    table.hasDataMapSchema =
        null != tableInfo.getDataMapSchemaList() && tableInfo.getDataMapSchemaList().size() > 0;
    setLocalDictInfo(table, tableInfo);
  }

  /**
   * This method sets whether the local dictionary is enabled or not, and the local dictionary
   * threshold, if not defined default value are considered.
   */
  private static void setLocalDictInfo(CarbonTable table, TableInfo tableInfo) {
    Map<String, String> tableProperties = tableInfo.getFactTable().getTableProperties();
    String isLocalDictionaryEnabled =
        tableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE);
    String localDictionaryThreshold =
        tableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD);
    if (null != isLocalDictionaryEnabled) {
      table.setLocalDictionaryEnabled(Boolean.parseBoolean(isLocalDictionaryEnabled));
      if (null != localDictionaryThreshold) {
        table.setLocalDictionaryThreshold(Integer.parseInt(localDictionaryThreshold));
      } else {
        table.setLocalDictionaryThreshold(
            Integer.parseInt(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT));
      }
    } else {
      // in case of old tables, local dictionary enable property will not be present in
      // tableProperties, so disable the local dictionary generation
      table.setLocalDictionaryEnabled(Boolean.parseBoolean("false"));
      tableProperties.put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE, "false");
    }
  }

  /**
   * Fill columns as per user provided order
   */
  private void fillCreateOrderColumn() {
    List<CarbonColumn> columns = new ArrayList<CarbonColumn>();
    columns.addAll(visibleDimensions);
    columns.addAll(visibleMeasures);
    Collections.sort(columns, new Comparator<CarbonColumn>() {

      @Override
      public int compare(CarbonColumn o1, CarbonColumn o2) {
        return Integer.compare(o1.getSchemaOrdinal(), o2.getSchemaOrdinal());
      }

    });
    this.createOrderColumn = columns;
  }

  /**
   * Fill allDimensions and allMeasures for carbon table
   */
  private void fillDimensionsAndMeasuresForTables(TableSchema tableSchema) {
    List<CarbonDimension> implicitDimensions = new ArrayList<CarbonDimension>();
    allDimensions = new ArrayList<CarbonDimension>();
    allMeasures = new ArrayList<CarbonMeasure>();
    this.implicitDimensions = implicitDimensions;
    int dimensionOrdinal = 0;
    int measureOrdinal = 0;
    int keyOrdinal = 0;
    List<ColumnSchema> listOfColumns = tableSchema.getListOfColumns();
    int complexTypeOrdinal = -1;
    for (int i = 0; i < listOfColumns.size(); i++) {
      ColumnSchema columnSchema = listOfColumns.get(i);
      if (columnSchema.isDimensionColumn()) {
        if (columnSchema.getNumberOfChild() > 0) {
          CarbonDimension complexDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, columnSchema.getSchemaOrdinal(),
                  -1, ++complexTypeOrdinal);
          complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          allDimensions.add(complexDimension);
          dimensionOrdinal =
              readAllComplexTypeChildrens(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  listOfColumns, complexDimension);
          i = dimensionOrdinal - 1;
          complexTypeOrdinal = assignComplexOrdinal(complexDimension, complexTypeOrdinal);
        } else {
          if (!columnSchema.isInvisible() && columnSchema.isSortColumn()) {
            this.numberOfSortColumns++;
          }
          if (!columnSchema.getEncodingList().contains(Encoding.DICTIONARY)) {
            CarbonDimension dimension = new CarbonDimension(columnSchema, dimensionOrdinal++,
                columnSchema.getSchemaOrdinal(), -1, -1);
            if (!columnSchema.isInvisible() && columnSchema.isSortColumn()) {
              this.numberOfNoDictSortColumns++;
            }
            allDimensions.add(dimension);
          } else if (columnSchema.getEncodingList().contains(Encoding.DICTIONARY)) {
            CarbonDimension dimension = new CarbonDimension(columnSchema, dimensionOrdinal++,
                columnSchema.getSchemaOrdinal(), keyOrdinal++, -1);
            allDimensions.add(dimension);
          }
        }
      } else {
        allMeasures.add(
            new CarbonMeasure(columnSchema, measureOrdinal++, columnSchema.getSchemaOrdinal()));
      }
    }
    fillVisibleDimensions();
    fillVisibleMeasures();
    addImplicitDimension(dimensionOrdinal, implicitDimensions);
    CarbonUtil.setLocalDictColumnsToWrapperSchema(tableSchema.getListOfColumns(),
        tableSchema.getTableProperties(),
        tableSchema.getTableProperties().get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE));
    dimensionOrdinalMax = dimensionOrdinal;
  }

  /**
   * This method will add implicit dimension into carbontable
   */
  private void addImplicitDimension(int dimensionOrdinal, List<CarbonDimension> dimensions) {
    dimensions.add(new CarbonImplicitDimension(dimensionOrdinal,
        CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_POSITIONID));
    dimensions.add(new CarbonImplicitDimension(dimensionOrdinal + 1,
        CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID));
  }

  /**
   * to get the all dimension of a table
   */
  public List<CarbonDimension> getImplicitDimensions() {
    return implicitDimensions;
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   */
  private int readAllComplexTypeChildrens(int dimensionOrdinal, int childCount,
      List<ColumnSchema> listOfColumns, CarbonDimension parentDimension) {
    for (int i = 0; i < childCount; i++) {
      ColumnSchema columnSchema = listOfColumns.get(dimensionOrdinal);
      if (columnSchema.isDimensionColumn()) {
        if (columnSchema.getNumberOfChild() > 0) {
          CarbonDimension complexDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, columnSchema.getSchemaOrdinal(),
                  -1, -1);
          complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          parentDimension.getListOfChildDimensions().add(complexDimension);
          dimensionOrdinal =
              readAllComplexTypeChildrens(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  listOfColumns, complexDimension);
        } else {
          CarbonDimension carbonDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, columnSchema.getSchemaOrdinal(),
                  -1, -1);
          parentDimension.getListOfChildDimensions().add(carbonDimension);
        }
      }
    }
    return dimensionOrdinal;
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
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
    return tableInfo.getDatabaseName();
  }

  /**
   * @return the tabelName
   */
  public String getTableName() {
    return tableInfo.getFactTable().getTableName();
  }

  /**
   * @return the tabelId
   */
  public String getTableId() {
    return tableInfo.getFactTable().getTableId();
  }

  /**
   * @return the tableUniqueName
   */
  public String getTableUniqueName() {
    return tableInfo.getTableUniqueName();
  }

  /**
   * Return true if local dictionary enabled for the table
   */
  public boolean isLocalDictionaryEnabled() {
    return isLocalDictionaryEnabled;
  }

  /**
   * set whether local dictionary enabled or not
   */
  private void setLocalDictionaryEnabled(boolean localDictionaryEnabled) {
    isLocalDictionaryEnabled = localDictionaryEnabled;
  }

  /**
   * @return local dictionary generation threshold
   */
  public int getLocalDictionaryThreshold() {
    return localDictionaryThreshold;
  }

  /**
   * set the local dictionary generation threshold
   */
  private void setLocalDictionaryThreshold(int localDictionaryThreshold) {
    this.localDictionaryThreshold = localDictionaryThreshold;
  }

  /**
   * Return the metadata path of the table
   */
  public String getMetadataPath() {
    return CarbonTablePath.getMetadataPath(getTablePath());
  }

  /**
   * Return the segment path of the specified segmentId
   */
  public String getSegmentPath(String segmentId) {
    return CarbonTablePath.getSegmentPath(getTablePath(), segmentId);
  }

  /**
   * @return store path
   */
  public String getTablePath() {
    return tableInfo.getOrCreateAbsoluteTableIdentifier().getTablePath();
  }

  /**
   * @return the tableLastUpdatedTime
   */
  public long getTableLastUpdatedTime() {
    return tableInfo.getLastUpdatedTime();
  }

  /**
   * Return all visible dimensions of the table
   */
  public List<CarbonDimension> getVisibleDimensions() {
    return visibleDimensions;
  }

  /**
   * Return all visible measure of the table
   */
  public List<CarbonMeasure> getVisibleMeasures() {
    return visibleMeasures;
  }

  /**
   * This will give user created order column
   */
  public List<CarbonColumn> getCreateOrderColumn() {
    return createOrderColumn;
  }

  /**
   * This method will give storage order column list
   */
  public List<CarbonColumn> getStreamStorageOrderColumn() {
    List<CarbonDimension> dimensions = visibleDimensions;
    List<CarbonMeasure> measures = visibleMeasures;
    List<CarbonColumn> columnList = new ArrayList<>(dimensions.size() + measures.size());
    List<CarbonColumn> complexDimensionList = new ArrayList<>(dimensions.size());
    for (CarbonColumn column : dimensions) {
      if (column.isComplex()) {
        complexDimensionList.add(column);
      } else {
        columnList.add(column);
      }
    }
    columnList.addAll(complexDimensionList);
    for (CarbonColumn column : measures) {
      if (!(column.getColName().equals("default_dummy_measure"))) {
        columnList.add(column);
      }
    }
    return columnList;
  }

  /**
   * Get particular measure
   */
  public CarbonMeasure getMeasureByName(String columnName) {
    for (CarbonMeasure measure : visibleMeasures) {
      if (measure.getColName().equalsIgnoreCase(columnName)) {
        return measure;
      }
    }
    return null;
  }

  /**
   * Get particular dimension
   */
  public CarbonDimension getDimensionByName(String columnName) {
    CarbonDimension carbonDimension = null;
    List<CarbonDimension> dimList = visibleDimensions;
    String[] colSplits = columnName.split("\\.");
    StringBuffer tempColName = new StringBuffer(colSplits[0]);
    for (String colSplit : colSplits) {
      if (!tempColName.toString().equalsIgnoreCase(colSplit)) {
        tempColName = tempColName.append(".").append(colSplit);
      }
      carbonDimension = getCarbonDimension(tempColName.toString(), dimList);
      if (carbonDimension != null && carbonDimension.getListOfChildDimensions() != null) {
        dimList = carbonDimension.getListOfChildDimensions();
      }
    }
    List<CarbonDimension> implicitDimList = implicitDimensions;
    if (carbonDimension == null) {
      carbonDimension = getCarbonDimension(columnName, implicitDimList);
    }

    if (colSplits.length > 1) {
      List<CarbonDimension> dimLists = visibleDimensions;
      for (CarbonDimension dims : dimLists) {
        if (dims.getColName().equalsIgnoreCase(colSplits[0])) {
          // Set the parent Dimension
          carbonDimension
              .setComplexParentDimension(getDimensionBasedOnOrdinal(dimLists, dims.getOrdinal()));
          break;
        }
      }
    }
    return carbonDimension;
  }

  private CarbonDimension getDimensionBasedOnOrdinal(List<CarbonDimension> dimList, int ordinal) {
    for (CarbonDimension dimension : dimList) {
      if (dimension.getOrdinal() == ordinal) {
        return dimension;
      }
    }
    throw new RuntimeException("No Dimension Matches the ordinal value");
  }

  /**
   * @return column by column name
   */
  public CarbonColumn getColumnByName(String columnName) {
    List<CarbonColumn> columns = createOrderColumn;
    Iterator<CarbonColumn> colItr = columns.iterator();
    while (colItr.hasNext()) {
      CarbonColumn col = colItr.next();
      if (col.getColName().equalsIgnoreCase(columnName)) {
        return col;
      }
    }
    return null;
  }

  /**
   * Returns all children dimension for complex type
   */
  public List<CarbonDimension> getChildren(String dimName) {
    return getChildren(dimName, visibleDimensions);
  }

  /**
   * Returns level 2 or more child allDimensions
   */
  private List<CarbonDimension> getChildren(String dimName, List<CarbonDimension> dimensions) {
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

  public BucketingInfo getBucketingInfo() {
    return bucket;
  }

  public PartitionInfo getPartitionInfo() {
    return partition;
  }

  public boolean isPartitionTable() {
    return null != partition
        && partition.getPartitionType() != PartitionType.NATIVE_HIVE;
  }

  public boolean isHivePartitionTable() {
    PartitionInfo partitionInfo = partition;
    return null != partitionInfo && partitionInfo.getPartitionType() == PartitionType.NATIVE_HIVE;
  }

  /**
   * @return absolute table identifier
   */
  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return tableInfo.getOrCreateAbsoluteTableIdentifier();
  }

  /**
   * @return carbon table identifier
   */
  public CarbonTableIdentifier getCarbonTableIdentifier() {
    return tableInfo.getOrCreateAbsoluteTableIdentifier().getCarbonTableIdentifier();
  }

  public int getBlockSizeInMB() {
    return tableInfo.getTableBlockSizeInMB();
  }

  public int getBlockletSizeInMB() {
    try {
      return Integer.parseInt(tableInfo.getFactTable().getTableProperties()
          .get(CarbonCommonConstants.TABLE_BLOCKLET_SIZE));
    } catch (NumberFormatException e) {
      return Integer.parseInt(CarbonCommonConstants.TABLE_BLOCKLET_SIZE_DEFAULT);
    }
  }

  /**
   * to get the normal dimension or the primitive dimension of the complex type
   *
   * @return primitive dimension of a table
   */
  public CarbonColumn getPrimitiveDimensionByName(String columnName) {
    for (CarbonDimension dim : visibleDimensions) {
      if (dim.getNumberOfChild() == 0 && dim.getColName().equalsIgnoreCase(columnName)) {
        return dim;
      }
    }
    return null;
  }

  /**
   * return all allDimensions in the table
   */
  public List<CarbonDimension> getAllDimensions() {
    return allDimensions;
  }

  /**
   * This method will all the visible allDimensions
   */
  private void fillVisibleDimensions() {
    List<CarbonDimension> visibleDimensions = new ArrayList<CarbonDimension>(allDimensions.size());
    for (CarbonDimension dimension : allDimensions) {
      if (!dimension.isInvisible()) {
        visibleDimensions.add(dimension);
        Map<String, String> columnProperties = dimension.getColumnProperties();
        if (columnProperties != null) {
          if (columnProperties.get(CarbonCommonConstants.COLUMN_DRIFT) != null) {
            columnDrift.add(dimension);
          }
        }
      }
    }
    this.visibleDimensions = visibleDimensions;
  }

  /**
   * return all allMeasures in the table
   */
  public List<CarbonMeasure> getAllMeasures() {
    return allMeasures;
  }

  public List<CarbonDimension> getColumnDrift() {
    return columnDrift;
  }

  public boolean hasColumnDrift() {
    return tableInfo.hasColumnDrift();
  }

  /**
   * This method will all the visible allMeasures
   */
  private void fillVisibleMeasures() {
    List<CarbonMeasure> visibleMeasures = new ArrayList<CarbonMeasure>(allMeasures.size());
    for (CarbonMeasure measure : allMeasures) {
      if (!measure.isInvisible()) {
        visibleMeasures.add(measure);
      }
    }
    this.visibleMeasures = visibleMeasures;
  }

  /**
   * Get the list of sort columns
   */
  public List<String> getSortColumns() {
    List<String> sortColumnsList = new ArrayList<String>(allDimensions.size());
    for (CarbonDimension dim : visibleDimensions) {
      if (dim.isSortColumn()) {
        sortColumnsList.add(dim.getColName());
      }
    }
    return sortColumnsList;
  }

  public int getNumberOfSortColumns() {
    return numberOfSortColumns;
  }

  public int getNumberOfNoDictSortColumns() {
    return numberOfNoDictSortColumns;
  }

  public static List<CarbonDimension> getNoDictSortColumns(List<CarbonDimension> dimensions) {
    List<CarbonDimension> noDictSortColumns = new ArrayList<>(dimensions.size());
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (dimension.isSortColumn() &&
          !dimension.getColumnSchema().hasEncoding(Encoding.DICTIONARY)) {
        noDictSortColumns.add(dimension);
      }
    }
    return noDictSortColumns;
  }

  public CarbonColumn getRangeColumn() {
    String rangeColumn =
        tableInfo.getFactTable().getTableProperties().get(CarbonCommonConstants.RANGE_COLUMN);
    if (rangeColumn == null) {
      return null;
    } else {
      return getColumnByName(rangeColumn);
    }
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  /**
   * Return true if this is a streaming table (table with property "streaming"="true" or "sink")
   */
  public boolean isStreamingSink() {
    String streaming = getTableInfo().getFactTable().getTableProperties().get("streaming");
    return streaming != null && (streaming.equalsIgnoreCase("true") || streaming
        .equalsIgnoreCase("sink"));
  }

  /**
   * Return true if this is a streaming source (table with property "streaming"="source")
   */
  public boolean isStreamingSource() {
    String streaming = getTableInfo().getFactTable().getTableProperties().get("streaming");
    return streaming != null && streaming.equalsIgnoreCase("source");
  }

  public int getDimensionOrdinalMax() {
    return dimensionOrdinalMax;
  }

  public boolean hasDataMapSchema() {
    return hasDataMapSchema;
  }

  public DataMapSchema getDataMapSchema(String dataMapName) {
    List<DataMapSchema> dataMaps = tableInfo.getDataMapSchemaList();
    for (DataMapSchema dataMap : dataMaps) {
      if (dataMap.getDataMapName().equalsIgnoreCase(dataMapName)) {
        return dataMap;
      }
    }
    return null;
  }

  public boolean isChildTableForMV() {
    return null != tableInfo.getFactTable().getTableProperties()
        .get(CarbonCommonConstants.PARENT_TABLES) && !tableInfo.getFactTable().getTableProperties()
        .get(CarbonCommonConstants.PARENT_TABLES).isEmpty();
  }

  /**
   * Return true if this is an external table (table with property "_external"="true", this is
   * an internal table property set during table creation)
   */
  public boolean isExternalTable() {
    String external = tableInfo.getFactTable().getTableProperties().get("_external");
    return external != null && external.equalsIgnoreCase("true");
  }

  public boolean isFileLevelFormat() {
    String external = tableInfo.getFactTable().getTableProperties().get("_filelevelformat");
    return external != null && external.equalsIgnoreCase("true");
  }

  public long size() throws IOException {
    Map<String, Long> dataIndexSize = CarbonUtil.calculateDataIndexSize(this, true);
    Long dataSize = dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE);
    if (dataSize == null) {
      dataSize = 0L;
    }
    Long indexSize = dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE);
    if (indexSize == null) {
      indexSize = 0L;
    }
    return dataSize + indexSize;
  }

  /**
   * Return true if this is a transactional table.
   * Transactional table means carbon will provide transactional support when user doing data
   * management like data loading, whether it is success or failure, data will be in consistent
   * state.
   * The difference between Transactional and non Transactional table is
   * non Transactional Table will not contain any Metadata folder and subsequently
   * no TableStatus or Schema files.
   */
  public boolean isTransactionalTable() {
    return tableInfo.isTransactionalTable();
  }

  public void setTransactionalTable(boolean transactionalTable) {
    tableInfo.setTransactionalTable(transactionalTable);
  }

  /**
   * methods returns true if operation is allowed for the corresponding datamap or not
   * if this operation makes datamap stale it is not allowed
   *
   * @param carbonTable carbontable to be operated
   * @param operation   which operation on the table,such as drop column,change datatype.
   * @param targets     objects which the operation impact on,such as column
   * @return true allow;false not allow
   */
  public boolean canAllow(CarbonTable carbonTable, TableOperation operation, Object... targets) {
    try {
      List<TableDataMap> datamaps = DataMapStoreManager.getInstance().getAllDataMap(carbonTable);
      if (!datamaps.isEmpty()) {
        for (TableDataMap dataMap : datamaps) {
          DataMapFactory factoryClass = DataMapStoreManager.getInstance()
              .getDataMapFactoryClass(carbonTable, dataMap.getDataMapSchema());
          if (factoryClass.willBecomeStale(operation)) {
            return false;
          }
          // check whether the operation is blocked for datamap
          if (factoryClass.isOperationBlocked(operation, targets)) {
            return false;
          }
        }
      }
    } catch (Exception e) {
      // since method returns true or false and based on that calling function throws exception, no
      // need to throw the catched exception
      LOGGER.error(e.getMessage(), e);
      return true;
    }
    return true;
  }

  /**
   * Get all index columns specified by dataMapSchema
   */
  public List<CarbonColumn> getIndexedColumns(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    String[] columns = dataMapSchema.getIndexColumns();
    List<CarbonColumn> indexColumn = new ArrayList<>(columns.length);
    for (String column : columns) {
      CarbonColumn carbonColumn = getColumnByName(column.trim().toLowerCase());
      if (carbonColumn == null) {
        throw new MalformedDataMapCommandException(String
            .format("column '%s' does not exist in table. Please check create DataMap statement.",
                column));
      }
      if (carbonColumn.getColName().isEmpty()) {
        throw new MalformedDataMapCommandException(
            CarbonCommonConstants.INDEX_COLUMNS + " contains invalid column name");
      }
      indexColumn.add(carbonColumn);
    }
    return indexColumn;
  }

  /**
   * Whether this table supports flat folder structure, it means all data files directly written
   * under table path
   */
  public boolean isSupportFlatFolder() {
    boolean supportFlatFolder = Boolean.parseBoolean(CarbonCommonConstants.DEFAULT_FLAT_FOLDER);
    Map<String, String> tblProps = getTableInfo().getFactTable().getTableProperties();
    if (tblProps.containsKey(CarbonCommonConstants.FLAT_FOLDER)) {
      supportFlatFolder = tblProps.get(CarbonCommonConstants.FLAT_FOLDER).equalsIgnoreCase("true");
    }
    return supportFlatFolder;
  }

  /**
   * Return the format value defined in table properties
   *
   * @return String as per table properties, null if not defined
   */
  public String getFormat() {
    return getTableInfo().getFactTable().getTableProperties().get("format");
  }

  /**
   * Method to get the list of cached columns of the table.
   * This method need to be used for Describe formatted like scenario where columns need to be
   * displayed in the column create order
   */
  public List<String> getMinMaxCachedColumnsInCreateOrder() {
    List<String> cachedColsList = new ArrayList<>();
    String cacheColumns =
        tableInfo.getFactTable().getTableProperties().get(CarbonCommonConstants.COLUMN_META_CACHE);
    if (null != cacheColumns) {
      if (!cacheColumns.isEmpty()) {
        String[] cachedCols = cacheColumns.split(",");
        for (String column : cachedCols) {
          CarbonColumn carbonColumn = getColumnByName(column);
          if (null != carbonColumn && !carbonColumn.isInvisible()) {
            cachedColsList.add(carbonColumn.getColName());
          }
        }
        return cachedColsList;
      } else {
        return new LinkedList<>();
      }
    } else {
      return Lists.newArrayList("All columns");
    }
  }

  /**
   * Method to find get carbon columns for columns to be cached. It will fill dimension first and
   * then measures based on the block segmentProperties.
   * In alter add column scenarios it can happen that the newly added columns are being cached
   * which do not exist in already loaded data. In those cases newly added columns should not be
   * cached for the already loaded data
   */
  public List<CarbonColumn> getMinMaxCacheColumns(SegmentProperties segmentProperties) {
    List<CarbonColumn> minMaxCachedColsList = null;
    String cacheColumns =
        tableInfo.getFactTable().getTableProperties().get(CarbonCommonConstants.COLUMN_META_CACHE);
    if (null != cacheColumns) {
      minMaxCachedColsList = new ArrayList<>();
      String[] cachedCols = cacheColumns.split(",");
      List<String> measureColumns = new ArrayList<>(cachedCols.length);
      List<CarbonDimension> complexDimensions = new ArrayList<>(cacheColumns.length());
      // add the columns in storage order: first normal dimensions, then complex dimensions
      // and then measures
      for (String column : cachedCols) {
        CarbonDimension dimension = getDimensionByName(column);
        // if found in dimension then add to dimension else add to measures
        if (null != dimension) {
          CarbonDimension dimensionFromCurrentBlock =
              segmentProperties.getDimensionFromCurrentBlock(dimension);
          if (null != dimensionFromCurrentBlock) {
            // first add normal dimensions and then complex dimensions
            if (dimensionFromCurrentBlock.isComplex()) {
              complexDimensions.add(dimensionFromCurrentBlock);
              continue;
            }
            minMaxCachedColsList.add(dimensionFromCurrentBlock);
          }
        } else {
          measureColumns.add(column);
        }
      }
      // add complex dimensions
      minMaxCachedColsList.addAll(complexDimensions);
      // search for measures columns and fill measures
      for (String measureColumn : measureColumns) {
        CarbonMeasure measure = getMeasureByName(measureColumn);
        if (null != measure) {
          CarbonMeasure measureFromCurrentBlock =
              segmentProperties.getMeasureFromCurrentBlock(measure);
          if (null != measureFromCurrentBlock) {
            minMaxCachedColsList.add(measureFromCurrentBlock);
          }
        }
      }
    }
    return minMaxCachedColsList;
  }

  /**
   * Return all inverted index columns in this table
   */
  public List<ColumnSchema> getInvertedIndexColumns() {
    if (getSortScope() == SortScopeOptions.SortScope.NO_SORT) {
      return new LinkedList<>();
    }
    List<ColumnSchema> columns = new LinkedList<>();
    for (ColumnSchema column : tableInfo.getFactTable().getListOfColumns()) {
      if (column.isUseInvertedIndex() && column.isSortColumn()) {
        columns.add(column);
      }
    }
    return columns;
  }

  /**
   * Return table level sort scope
   */
  public SortScopeOptions.SortScope getSortScope() {
    String sortScope = tableInfo.getFactTable().getTableProperties().get("sort_scope");
    if (sortScope == null) {
      if (getNumberOfSortColumns() == 0) {
        return SortScopeOptions.SortScope.NO_SORT;
      } else {
        // Check SORT_SCOPE in Session Properties first.
        String sortScopeSessionProp = CarbonProperties.getInstance().getProperty(
            CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE + getDatabaseName() + "."
                + getTableName());
        if (null != sortScopeSessionProp) {
          return SortScopeOptions.getSortScope(sortScopeSessionProp);
        }

        // If SORT_SCOPE is not found in Session Properties,
        // then retrieve it from Table.
        return SortScopeOptions.getSortScope(CarbonProperties.getInstance()
            .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
                CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, "LOCAL_SORT")));
      }
    } else {
      return SortScopeOptions.getSortScope(sortScope);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    tableInfo.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tableInfo = new TableInfo();
    tableInfo.readFields(in);
    updateTableByTableInfo(this, tableInfo);
  }
}
