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
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

/**
 * BloomDataMap is constructed in CG level (blocklet level).
 * For each indexed column, a bloom filter is constructed to indicate whether a value
 * belongs to this blocklet. Bloom filter of blocklet that belongs to same block will
 * be written to one index file suffixed with .bloomindex. So the number
 * of bloom index file will be equal to that of the blocks.
 */
@InterfaceAudience.Internal
public class BloomDataMapWriter extends AbstractBloomDataMapWriter {

  BloomDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, SegmentProperties segmentProperties,
      int bloomFilterSize, double bloomFilterFpp, boolean compressBloom)
      throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName, segmentProperties,
        bloomFilterSize, bloomFilterFpp, compressBloom);
  }

  protected byte[] convertNonDictionaryValue(int indexColIdx, byte[] value) {
    if (DataTypes.VARCHAR == indexColumns.get(indexColIdx).getDataType()) {
      return DataConvertUtil.getRawBytesForVarchar(value);
    } else {
      return DataConvertUtil.getRawBytes(value);
    }
  }
}
