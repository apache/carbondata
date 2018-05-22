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

package org.apache.carbondata.datamap.bloom;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.util.bloom.Key;

/**
 * Implementation for BloomFilter DataMap to rebuild the datamap for main table with existing data
 */
@InterfaceAudience.Internal
public class BloomDataMapBuilder extends BloomDataMapWriter implements DataMapBuilder {

  BloomDataMapBuilder(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, int bloomFilterSize, double bloomFilterFpp,
      boolean bloomCompress) throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName, bloomFilterSize, bloomFilterFpp,
        bloomCompress);
  }

  @Override
  public void initialize() throws IOException {
    super.resetBloomFilters();
  }

  @Override
  public void addRow(int blockletId, int pageId, int rowId, Object[] values) {
    if (currentBlockletId != blockletId) {
      // new blocklet started, flush bloom filter to datamap fileh
      super.writeBloomDataMapFile();
      currentBlockletId = blockletId;
    }
    // for each indexed column, add the data to bloom filter
    List<CarbonColumn> indexColumns = getIndexColumns();
    for (int i = 0; i < indexColumns.size(); i++) {
      Object data = values[i];
      DataType dataType = indexColumns.get(i).getDataType();
      byte[] indexValue;
      if (DataTypes.STRING == dataType) {
        indexValue = getStringData(data);
      } else if (DataTypes.BYTE_ARRAY == dataType) {
        byte[] originValue = (byte[]) data;
        // String and byte array is LV encoded, L is short type
        indexValue = new byte[originValue.length - 2];
        System.arraycopy(originValue, 2, indexValue, 0, originValue.length - 2);
      } else {
        indexValue = CarbonUtil.getValueAsBytes(dataType, data);
      }
      indexBloomFilters.get(i).add(new Key(indexValue));
    }
  }

  @Override
  public void finish() throws IOException {
    super.finish();
  }

  @Override
  public void close() throws IOException {
    releaseResouce();
  }

  @Override
  protected byte[] getStringData(Object data) {
    return ((String) data).getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
  }
}
