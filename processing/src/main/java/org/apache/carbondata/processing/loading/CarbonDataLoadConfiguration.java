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

package org.apache.carbondata.processing.loading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.SortColumnRangeInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.OutputFilesInfoHolder;

public class CarbonDataLoadConfiguration {

  private DataField[] dataFields;

  private AbsoluteTableIdentifier tableIdentifier;

  private String[] header;

  private String segmentId;

  private String taskNo;

  private BucketingInfo bucketingInfo;

  private String segmentPath;

  private Map<String, Object> dataLoadProperties = new HashMap<>();

  private boolean preFetch;

  private int dimensionCount;

  private int measureCount;

  private int noDictionaryCount;

  private int complexDictionaryColumnCount;

  private int complexNonDictionaryColumnCount;

  /**
   * schema updated time stamp to be used for restructure scenarios
   */
  private long schemaUpdatedTimeStamp;

  private int numberOfSortColumns;

  private int numberOfNoDictSortColumns;

  // contains metadata used in write step of loading process
  private TableSpec tableSpec;

  /**
   * Number of thread cores to use while writing data files
   */
  private short writingCoresCount;

  private SortColumnRangeInfo sortColumnRangeInfo;

  private boolean carbonTransactionalTable;

  /**
   * Folder path to where data should be written for this load.
   */
  private String dataWritePath;

  /**
   * name of compressor to be used to compress column page
   */
  private String columnCompressor;

  private int numberOfLoadingCores;

  private OutputFilesInfoHolder outputFilesInfoHolder;

  /**
   * Whether index columns are present. This flag should be set only when all the schema
   * columns are already converted. Now, just need to generate and convert index columns present in
   * data fields.
   */
  private boolean isIndexColumnsPresent;

  public CarbonDataLoadConfiguration() {
  }

  public void setDataFields(DataField[] dataFields) {
    this.dataFields = dataFields;

    // set counts for each column category
    for (DataField dataField : dataFields) {
      CarbonColumn column = dataField.getColumn();
      if (column.isDimension()) {
        dimensionCount++;
        if (column.isComplex()) {
          if (!dataField.isDateDataType()) {
            complexNonDictionaryColumnCount++;
          } else {
            complexDictionaryColumnCount++;
          }
        } else if (!dataField.isDateDataType()) {
          noDictionaryCount++;
        }
      }

      if (column.isMeasure()) {
        measureCount++;
      }
    }
  }

  public DataField[] getDataFields() {
    return dataFields;
  }

  public int getDimensionCount() {
    return dimensionCount;
  }

  public int getNoDictionaryCount() {
    return noDictionaryCount;
  }

  public int getComplexDictionaryColumnCount() {
    return complexDictionaryColumnCount;
  }

  public int getMeasureCount() {
    return measureCount;
  }

  public void setNumberOfSortColumns(int numberOfSortColumns) {
    this.numberOfSortColumns = numberOfSortColumns;
  }

  public int getNumberOfSortColumns() {
    return this.numberOfSortColumns;
  }

  public boolean isSortTable() {
    return this.numberOfSortColumns > 0;
  }

  public void setNumberOfNoDictSortColumns(int numberOfNoDictSortColumns) {
    this.numberOfNoDictSortColumns = numberOfNoDictSortColumns;
  }

  public int getNumberOfNoDictSortColumns() {
    return this.numberOfNoDictSortColumns;
  }

  public String[] getHeader() {
    return header;
  }

  public void setHeader(String[] header) {
    this.header = header;
  }

  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(AbsoluteTableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  public String getTaskNo() {
    return taskNo;
  }

  public void setTaskNo(String taskNo) {
    this.taskNo = taskNo;
  }

  public void setDataLoadProperty(String key, Object value) {
    dataLoadProperties.put(key, value);
  }

  public Object getDataLoadProperty(String key) {
    return dataLoadProperties.get(key);
  }

  public BucketingInfo getBucketingInfo() {
    return bucketingInfo;
  }

  public void setBucketingInfo(BucketingInfo bucketingInfo) {
    this.bucketingInfo = bucketingInfo;
  }

  public boolean isPreFetch() {
    return preFetch;
  }

  public void setPreFetch(boolean preFetch) {
    this.preFetch = preFetch;
  }

  public long getSchemaUpdatedTimeStamp() {
    return schemaUpdatedTimeStamp;
  }

  public void setSchemaUpdatedTimeStamp(long schemaUpdatedTimeStamp) {
    this.schemaUpdatedTimeStamp = schemaUpdatedTimeStamp;
  }

  public DataType[] getMeasureDataType() {
    // data field might be rearranged in case of partition.
    // so refer internal order not the data field order.
    List<CarbonMeasure> visibleMeasures = tableSpec.getCarbonTable().getVisibleMeasures();
    DataType[] type = new DataType[visibleMeasures.size()];
    for (int i = 0; i < type.length; i++) {
      type[i] = visibleMeasures.get(i).getDataType();
    }
    return type;
  }

  /**
   * Get the data types of the no dictionary and the complex dimensions of the table
   *
   * @return
   */
  public CarbonColumn[] getNoDictAndComplexDimensions() {
    // data field might be rearranged in case of partition.
    // so refer internal order not the data field order.
    List<CarbonDimension> visibleDimensions = tableSpec.getCarbonTable().getVisibleDimensions();
    List<CarbonColumn> noDictionaryDimensions = new ArrayList<>();
    for (int i = 0; i < visibleDimensions.size(); i++) {
      if (visibleDimensions.get(i).getDataType() != DataTypes.DATE) {
        noDictionaryDimensions.add(visibleDimensions.get(i));
      }
    }
    return noDictionaryDimensions.toArray(new CarbonColumn[0]);
  }

  /**
   * Get the sort column mapping of the table
   *
   * @return
   */
  public boolean[] getSortColumnMapping() {
    boolean[] sortColumnMapping = new boolean[dataFields.length];
    for (int i = 0; i < sortColumnMapping.length; i++) {
      if (dataFields[i].getColumn().getColumnSchema().isSortColumn()) {
        sortColumnMapping[i] = true;
      }
    }
    return sortColumnMapping;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  public void setTableSpec(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  public short getWritingCoresCount() {
    return writingCoresCount;
  }

  public void setWritingCoresCount(short writingCoresCount) {
    this.writingCoresCount = writingCoresCount;
  }

  public String getDataWritePath() {
    return dataWritePath;
  }

  public void setDataWritePath(String dataWritePath) {
    this.dataWritePath = dataWritePath;
  }

  public SortColumnRangeInfo getSortColumnRangeInfo() {
    return sortColumnRangeInfo;
  }

  public void setSortColumnRangeInfo(SortColumnRangeInfo sortColumnRangeInfo) {
    this.sortColumnRangeInfo = sortColumnRangeInfo;
  }

  public boolean isCarbonTransactionalTable() {
    return carbonTransactionalTable;
  }

  public void setCarbonTransactionalTable(boolean carbonTransactionalTable) {
    this.carbonTransactionalTable = carbonTransactionalTable;
  }

  public int getComplexNonDictionaryColumnCount() {
    return complexNonDictionaryColumnCount;
  }

  public String getColumnCompressor() {
    return columnCompressor;
  }

  public void setColumnCompressor(String columnCompressor) {
    this.columnCompressor = columnCompressor;
  }

  public int getNumberOfLoadingCores() {
    return numberOfLoadingCores;
  }

  public void setNumberOfLoadingCores(int numberOfLoadingCores) {
    this.numberOfLoadingCores = numberOfLoadingCores;
  }

  public String getSegmentPath() {
    return segmentPath;
  }

  public void setSegmentPath(String segmentPath) {
    this.segmentPath = segmentPath;
  }

  public OutputFilesInfoHolder getOutputFilesInfoHolder() {
    return outputFilesInfoHolder;
  }

  public void setOutputFilesInfoHolder(OutputFilesInfoHolder outputFilesInfoHolder) {
    this.outputFilesInfoHolder = outputFilesInfoHolder;
  }

  public boolean isIndexColumnsPresent() {
    return isIndexColumnsPresent;
  }

  public void setIndexColumnsPresent(boolean indexColumnsPresent) {
    isIndexColumnsPresent = indexColumnsPresent;
  }
}
