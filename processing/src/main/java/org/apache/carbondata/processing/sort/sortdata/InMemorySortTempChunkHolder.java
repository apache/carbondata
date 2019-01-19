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
package org.apache.carbondata.processing.sort.sortdata;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;

/**
 * Adapter for RowResultIterator. This will be used for sorted
 * carbondata files during compaction.
 */
public class InMemorySortTempChunkHolder extends SortTempFileChunkHolder {

  /**
   * Iterator over carbondata file
   */
  private final RawResultIterator rawResultIterator;

  /**
   * Used to convert RawResultItertor output row to CarbonRow
   */
  private SegmentProperties segmentProperties;

  /**
   * Used to convert RawResultItertor output row to CarbonRow
   */
  private CarbonColumn[] noDicAndComplexColumns;

  /**
   * Used to store Measure Data Type
   */
  private DataType[] measureDataType;

  public InMemorySortTempChunkHolder(RawResultIterator rawResultIterator,
      SegmentProperties segmentProperties, CarbonColumn[] noDicAndComplexColumns,
      SortParameters sortParameters, DataType[] measureDataType) {
    super(sortParameters);
    this.rawResultIterator = rawResultIterator;
    this.segmentProperties = segmentProperties;
    this.noDicAndComplexColumns = noDicAndComplexColumns;
    this.measureDataType = measureDataType;
  }

  public void initialise() {
    // Not required for In memory case as it will not initialize anything
    throw new UnsupportedOperationException("Operation Not supported");
  }

  /**
   * 1. Read row from RawResultIterator'
   * 2. Convert it to IntermediateSortTempRow
   * 3. Store it in memory to read through getRow() method
   */
  public void readRow() {
    Object[] row = this.rawResultIterator.next();
    //TODO add code to get directly Object[] Instead Of CarbonRow Object
    CarbonRow carbonRow =
        WriteStepRowUtil.fromMergerRow(row, segmentProperties, noDicAndComplexColumns);
    Object[] data = carbonRow.getData();
    Object[] measuresValue = (Object[]) data[WriteStepRowUtil.MEASURE];
    for (int i = 0; i < measuresValue.length; i++) {
      measuresValue[i] = getConvertedMeasureValue(measuresValue[i], measureDataType[i]);
    }
    returnRow = new IntermediateSortTempRow((int[]) data[WriteStepRowUtil.DICTIONARY_DIMENSION],
        (Object[]) data[WriteStepRowUtil.NO_DICTIONARY_AND_COMPLEX], measuresValue);

  }

  public int getEntryCount() {
    // this will not be used for intermediate sorting
    throw new UnsupportedOperationException("Operation Not supported");
  }

  /**
   * below method will be used to check whether any more records are present
   * in file or not
   *
   * @return more row present in file
   */
  public boolean hasNext() {
    return this.rawResultIterator.hasNext();
  }

  @Override public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override public int hashCode() {
    int hash = rawResultIterator.hashCode();
    hash += segmentProperties.hashCode();
    return hash;
  }

  /**
   * Below method will be used to close streams
   */
  public void closeStream() {
    rawResultIterator.close();
  }

  /* below method will be used to get the sort temp row
   *
   * @return row
   */
  public IntermediateSortTempRow getRow() {
    return returnRow;
  }

  /**
   * This method will convert the spark decimal to java big decimal type
   *
   * @param value
   * @param type
   * @return
   */
  private Object getConvertedMeasureValue(Object value, DataType type) {
    if (DataTypes.isDecimal(type)) {
      if (value != null) {
        value = DataTypeUtil.getDataTypeConverter().convertFromDecimalToBigDecimal(value);
      }
      return value;
    } else {
      return value;
    }
  }
}
