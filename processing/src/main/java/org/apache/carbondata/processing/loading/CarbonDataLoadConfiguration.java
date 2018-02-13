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
import org.apache.carbondata.core.dictionary.service.DictionaryServiceProvider;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.SortColumnRangeInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.processing.loading.converter.DictionaryCardinalityFinder;

public class CarbonDataLoadConfiguration {

  private DataField[] dataFields;

  private AbsoluteTableIdentifier tableIdentifier;

  private String[] header;

  private String segmentId;

  private String taskNo;

  private BucketingInfo bucketingInfo;

  private Map<String, Object> dataLoadProperties = new HashMap<>();

  /**
   *  Use one pass to generate dictionary
   */
  private boolean useOnePass;

  /**
   * dictionary server host
   */
  private String dictionaryServerHost;

  /**
   * dictionary sever port
   */
  private int dictionaryServerPort;

  /**
   * dictionary server secret key
   */
  private String dictionaryServerSecretKey;

  /**
   * Dictionary Service Provider.
   */
  private DictionaryServiceProvider dictionaryServiceProvider;

  /**
   * Secure Mode or not.
   */
  private Boolean dictionaryEncryptServerSecure;

  private boolean preFetch;

  private int dimensionCount;

  private int measureCount;

  private int noDictionaryCount;

  private int complexColumnCount;

  /**
   * schema updated time stamp to be used for restructure scenarios
   */
  private long schemaUpdatedTimeStamp;

  private DictionaryCardinalityFinder cardinalityFinder;

  private int numberOfSortColumns;

  private int numberOfNoDictSortColumns;

  // contains metadata used in write step of loading process
  private TableSpec tableSpec;

  /**
   * Number of thread cores to use while writing data files
   */
  private short writingCoresCount;

  private SortColumnRangeInfo sortColumnRangeInfo;

  /**
   * Flder path to where data should be written for this load.
   */
  private String dataWritePath;

  public CarbonDataLoadConfiguration() {
  }

  public void setDataFields(DataField[] dataFields) {
    this.dataFields = dataFields;

    // set counts for each column category
    for (DataField dataField : dataFields) {
      CarbonColumn column = dataField.getColumn();
      if (column.isDimension()) {
        dimensionCount++;
        if (!dataField.hasDictionaryEncoding()) {
          noDictionaryCount++;
        }
      }
      if (column.isComplex()) {
        complexColumnCount++;
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

  public int getComplexColumnCount() {
    return complexColumnCount;
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

  public boolean getUseOnePass() {
    return useOnePass;
  }

  public void setUseOnePass(boolean useOnePass) {
    this.useOnePass = useOnePass;
  }

  public String getDictionaryServerHost() {
    return dictionaryServerHost;
  }

  public void setDictionaryServerHost(String dictionaryServerHost) {
    this.dictionaryServerHost = dictionaryServerHost;
  }

  public int getDictionaryServerPort() {
    return dictionaryServerPort;
  }

  public void setDictionaryServerPort(int dictionaryServerPort) {
    this.dictionaryServerPort = dictionaryServerPort;
  }

  public String getDictionaryServerSecretKey() {
    return dictionaryServerSecretKey;
  }

  public void setDictionaryServerSecretKey(String dictionaryServerSecretKey) {
    this.dictionaryServerSecretKey = dictionaryServerSecretKey;
  }

  public DictionaryServiceProvider getDictionaryServiceProvider() {
    return dictionaryServiceProvider;
  }

  public void setDictionaryServiceProvider(DictionaryServiceProvider dictionaryServiceProvider) {
    this.dictionaryServiceProvider = dictionaryServiceProvider;
  }

  public Boolean getDictionaryEncryptServerSecure() {
    return dictionaryEncryptServerSecure;
  }

  public void setDictionaryEncryptServerSecure(Boolean dictionaryEncryptServerSecure) {
    this.dictionaryEncryptServerSecure = dictionaryEncryptServerSecure;
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

  public DictionaryCardinalityFinder getCardinalityFinder() {
    return cardinalityFinder;
  }

  public void setCardinalityFinder(DictionaryCardinalityFinder cardinalityFinder) {
    this.cardinalityFinder = cardinalityFinder;
  }

  public DataType[] getMeasureDataType() {
    List<Integer> measureIndexes = new ArrayList<>(dataFields.length);
    int measureCount = 0;
    for (int i = 0; i < dataFields.length; i++) {
      if (!dataFields[i].getColumn().isDimension()) {
        measureIndexes.add(i);
        measureCount++;
      }
    }

    DataType[] type = new DataType[measureCount];
    for (int i = 0; i < type.length; i++) {
      type[i] = dataFields[measureIndexes.get(i)].getColumn().getDataType();
    }
    return type;
  }

  public int[] calcDimensionLengths() {
    int[] dimLensWithComplex = getCardinalityFinder().getCardinality();
    if (!isSortTable()) {
      for (int i = 0; i < dimLensWithComplex.length; i++) {
        if (dimLensWithComplex[i] != 0) {
          dimLensWithComplex[i] = Integer.MAX_VALUE;
        }
      }
    }
    List<Integer> dimsLenList = new ArrayList<Integer>();
    for (int eachDimLen : dimLensWithComplex) {
      if (eachDimLen != 0) dimsLenList.add(eachDimLen);
    }
    int[] dimLens = new int[dimsLenList.size()];
    for (int i = 0; i < dimsLenList.size(); i++) {
      dimLens[i] = dimsLenList.get(i);
    }
    return dimLens;
  }

  public KeyGenerator[] createKeyGeneratorForComplexDimension() {
    int[] dimLens = calcDimensionLengths();
    KeyGenerator[] complexKeyGenerators = new KeyGenerator[dimLens.length];
    for (int i = 0; i < dimLens.length; i++) {
      complexKeyGenerators[i] =
          KeyGeneratorFactory.getKeyGenerator(new int[] { dimLens[i] });
    }
    return complexKeyGenerators;
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
}
