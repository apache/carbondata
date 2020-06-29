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

package org.apache.carbondata.index.bloom;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.IndexBuilder;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Implementation for BloomFilter Index to rebuild the indes for main table with existing data
 */
@InterfaceAudience.Internal
public class BloomIndexBuilder extends AbstractBloomIndexWriter implements IndexBuilder {

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
  BloomIndexBuilder(String tablePath, String indexName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, int bloomFilterSize, double bloomFilterFpp,
      boolean bloomCompress) throws IOException {
    super(tablePath, indexName, indexColumns, segment, shardName, bloomFilterSize,
        bloomFilterFpp, bloomCompress);
  }

  @Override
  public void initialize() {
    super.resetBloomFilters();
  }

  @Override
  public void addRow(int blockletId, int pageId, int rowId, Object[] values) {
    if (currentBlockletId != blockletId) {
      // new blocklet started, flush bloom filter to index fileh
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
      super.writeBloomIndexFile();
      currentBlockletId = blockletId;
    }
    // for each indexed column, add the data to bloom filter
    for (int i = 0; i < indexColumns.size(); i++) {
      Object data = values[i];
      addValue2BloomIndex(i, data);
    }
  }

  @Override
  protected byte[] convertNonDictionaryValue(int indexColIdx, Object value) {
    // no dictionary measure columns will be of original data, so convert it to bytes
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    if (DataTypeUtil.isPrimitiveColumn(indexColumns.get(indexColIdx).getDataType())) {
      return CarbonUtil.getValueAsBytes(indexColumns.get(indexColIdx).getDataType(), value);
    }
    return (byte[]) value;
  }

  @Override
  public void finish() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2653
    if (!isWritingFinished()) {
      if (indexBloomFilters.size() > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        writeBloomIndexFile();
      }
      releaseResouce();
      setWritingFinished(true);
    }
  }

  @Override
  protected byte[] convertDictionaryValue(int indexColIdx, Object value) {
    // input value from IndexIndexRebuildRDD is already decoded as surrogate key
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2727
    return CarbonUtil.getValueAsBytes(DataTypes.INT, value);
  }

  @Override
  public void close() {
    releaseResouce();
  }

  @Override
  public boolean isIndexForCarbonRawBytes() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2637
    return true;
  }
}
