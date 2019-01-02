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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

/**
 * BloomDataMap is constructed in CG level (blocklet level).
 * For each indexed column, a bloom filter is constructed to indicate whether a value
 * belongs to this blocklet. Bloom filter of blocklet that belongs to same block will
 * be written to one index file suffixed with .bloomindex. So the number
 * of bloom index file will be equal to that of the blocks.
 */
@InterfaceAudience.Internal
public class BloomDataMapWriter extends AbstractBloomDataMapWriter {
  private ColumnarSplitter columnarSplitter;
  // for the dict/sort/date column, they are encoded in MDK,
  // this maps the index column name to the index in MDK
  private Map<String, Integer> indexCol2MdkIdx;

  BloomDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, SegmentProperties segmentProperties,
      int bloomFilterSize, double bloomFilterFpp, boolean compressBloom)
      throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName, segmentProperties,
        bloomFilterSize, bloomFilterFpp, compressBloom);

    columnarSplitter = segmentProperties.getFixedLengthKeySplitter();
    this.indexCol2MdkIdx = new HashMap<>();
    int idx = 0;
    for (final CarbonDimension dimension : segmentProperties.getDimensions()) {
      if (!dimension.isGlobalDictionaryEncoding() && !dimension.isDirectDictionaryEncoding()) {
        continue;
      }
      boolean isExistInIndex = CollectionUtils.exists(indexColumns, new Predicate() {
        @Override public boolean evaluate(Object object) {
          return ((CarbonColumn) object).getColName().equalsIgnoreCase(dimension.getColName());
        }
      });
      if (isExistInIndex) {
        this.indexCol2MdkIdx.put(dimension.getColName(), idx);
      }
      idx++;
    }
  }

  protected byte[] convertNonDictionaryValue(int indexColIdx, Object value) {
    if (DataTypes.VARCHAR == indexColumns.get(indexColIdx).getDataType()) {
      return DataConvertUtil.getRawBytesForVarchar((byte[]) value);
    } else if (DataTypeUtil.isPrimitiveColumn(indexColumns.get(indexColIdx).getDataType())) {
      // get bytes for the original value of the no dictionary column
      return CarbonUtil.getValueAsBytes(indexColumns.get(indexColIdx).getDataType(), value);
    } else {
      return DataConvertUtil.getRawBytes((byte[]) value);
    }
  }

  @Override
  protected byte[] convertDictionaryValue(int indexColIdx, Object value) {
    // input value from onPageAdded in load process is byte[]

    // for dict columns including dictionary and date columns decode value to get the surrogate key
    int thisKeyIdx = indexCol2MdkIdx.get(indexColumns.get(indexColIdx).getColName());
    int surrogateKey = CarbonUtil.getSurrogateInternal((byte[]) value, 0,
        columnarSplitter.getBlockKeySize()[thisKeyIdx]);
    // store the dictionary key in bloom
    return CarbonUtil.getValueAsBytes(DataTypes.INT, surrogateKey);
  }
}
