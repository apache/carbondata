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
package org.apache.carbondata.core.scan.result.iterator;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.result.RowBatch;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.log4j.Logger;

/**
 * This is a wrapper iterator over the detail raw query iterator.
 * This iterator will handle the processing of the raw rows.
 * This will handle the batch results and will iterate on the batches and give single row.
 */
public class ColumnDriftRawResultIterator extends RawResultIterator {

  // column reorder for no-dictionary column
  private int noDictCount;
  private int[] noDictMap;
  // column drift
  private boolean[] isColumnDrift;
  private int measureCount;
  private DataType[] measureDataTypes;

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ColumnDriftRawResultIterator.class.getName());

  public ColumnDriftRawResultIterator(CarbonIterator<RowBatch> detailRawQueryResultIterator,
      SegmentProperties sourceSegProperties, SegmentProperties destinationSegProperties) {
    super(detailRawQueryResultIterator, sourceSegProperties, destinationSegProperties, false);
    initForColumnDrift();
    init();
  }

  private void initForColumnDrift() {
    List<CarbonDimension> noDictDims =
        new ArrayList<>(destinationSegProperties.getDimensions().size());
    for (CarbonDimension dimension : destinationSegProperties.getDimensions()) {
      if (dimension.getNumberOfChild() == 0) {
        if (!dimension.hasEncoding(Encoding.DICTIONARY)) {
          noDictDims.add(dimension);
        }
      }
    }
    measureCount = destinationSegProperties.getMeasures().size();
    noDictCount = noDictDims.size();
    isColumnDrift = new boolean[noDictCount];
    noDictMap = new int[noDictCount];
    measureDataTypes = new DataType[noDictCount];
    List<CarbonMeasure> sourceMeasures = sourceSegProperties.getMeasures();
    int tableMeasureCount = sourceMeasures.size();
    for (int i = 0; i < noDictCount; i++) {
      for (int j = 0; j < tableMeasureCount; j++) {
        if (RestructureUtil.isColumnMatches(true, noDictDims.get(i), sourceMeasures.get(j))) {
          isColumnDrift[i] = true;
          measureDataTypes[i] = sourceMeasures.get(j).getDataType();
          break;
        }
      }
    }
    int noDictIndex = 0;
    // the column drift are at the end of measures
    int measureIndex = measureCount + 1;
    for (int i = 0; i < noDictCount; i++) {
      if (isColumnDrift[i]) {
        noDictMap[i] = measureIndex++;
      } else {
        noDictMap[i] = noDictIndex++;
      }
    }
  }

  @Override
  protected Object[] convertRow(Object[] rawRow) throws KeyGenException {
    super.convertRow(rawRow);
    ByteArrayWrapper dimObject = (ByteArrayWrapper) rawRow[0];
    // need move measure to dimension and return new row by current schema
    byte[][] noDicts = dimObject.getNoDictionaryKeys();
    byte[][] newNoDicts = new byte[noDictCount][];
    for (int i = 0; i < noDictCount; i++) {
      if (isColumnDrift[i]) {
        newNoDicts[i] = DataTypeUtil
            .getBytesDataDataTypeForNoDictionaryColumn(rawRow[noDictMap[i]], measureDataTypes[i]);
      } else {
        newNoDicts[i] = noDicts[noDictMap[i]];
      }
    }
    ByteArrayWrapper newWrapper = new ByteArrayWrapper();
    newWrapper.setDictionaryKey(dimObject.getDictionaryKey());
    newWrapper.setNoDictionaryKeys(newNoDicts);
    newWrapper.setComplexTypesKeys(dimObject.getComplexTypesKeys());
    newWrapper.setImplicitColumnByteArray(dimObject.getImplicitColumnByteArray());
    Object[] finalRawRow = new Object[1 + measureCount];
    finalRawRow[0] = newWrapper;
    System.arraycopy(rawRow, 1, finalRawRow, 1, measureCount);
    return finalRawRow;
  }
}
