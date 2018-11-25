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

package org.apache.carbondata.datamap.minmax;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.filter.FilterUtil;

public class MinMaxDataMapBuilder extends AbstractMinMaxDataMapWriter implements DataMapBuilder {
  private KeyGenerator keyGenerator;

  MinMaxDataMapBuilder(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, SegmentProperties segmentProperties) throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
    for (CarbonColumn col : indexColumns) {
      if (col.hasEncoding(Encoding.DICTIONARY)) {
        initKeyGenerator(segmentProperties);
        break;
      }
    }
  }

  private void initKeyGenerator(SegmentProperties segmentProperties) {
    keyGenerator = segmentProperties.getDimensionKeyGenerator();
  }

  @Override
  public void initialize() throws IOException {
    super.resetBlockletLevelMinMax();
  }

  @Override
  public void addRow(int blockletId, int pageId, int rowId, Object[] values) {
    if (currentBlockletId != blockletId) {
      // new blocklet started, flush bloom filter to datamap fileh
      super.flushMinMaxIndexFile();
      currentBlockletId = blockletId;
    }
    // for each indexed column, add the data to bloom filter
    for (int i = 0; i < indexColumns.size(); i++) {
      Object data = values[i];
      updateBlockletMinMax(i, data);
    }
  }

  @Override
  protected byte[] convertNonDicValueToPlain(int indexColIdx, byte[] value) {
    return value;
  }

  @Override
  public void finish() throws IOException {
    if (!isWritingFinished()) {
      flushMinMaxIndexFile();
      releaseResource();
      setWritingFinished(true);
    }
  }

  @Override
  protected byte[] convertDictValueToMdk(int indexColIdx, Object value) {
    // input value from IndexDataMapRebuildRDD is already decoded as surrogate key
    // we need to convert the surrogate key to MDK now
    CarbonColumn carbonColumn = indexColumns.get(indexColIdx);
    assert (carbonColumn instanceof CarbonDimension);
    return FilterUtil.getMaskKey((int) value, (CarbonDimension) carbonColumn, keyGenerator);
  }

  @Override
  public void close() throws IOException {
    releaseResource();
  }

  @Override
  public boolean isIndexForCarbonRawBytes() {
    return true;
  }
}
