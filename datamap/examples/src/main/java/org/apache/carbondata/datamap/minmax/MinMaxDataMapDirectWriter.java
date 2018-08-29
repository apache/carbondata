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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

@InterfaceAudience.Internal
public class MinMaxDataMapDirectWriter extends AbstractMinMaxDataMapWriter {

  MinMaxDataMapDirectWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, SegmentProperties segmentProperties) throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
  }

  protected byte[] convertNonDicValueToPlain(int indexColIdx, byte[] lvData) {
    int lenInLV = (DataTypes.VARCHAR == indexColumns.get(indexColIdx).getDataType()) ?
        CarbonCommonConstants.INT_SIZE_IN_BYTE : CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    byte[] indexValue = new byte[lvData.length - lenInLV];
    System.arraycopy(lvData, lenInLV, indexValue, 0, lvData.length - lenInLV);
    return indexValue;
  }

  @Override
  protected byte[] convertDictValueToMdk(int indexColIdx, Object value) {
    // input value from onPageAdded in load process is byte[]
    return (byte[]) value;
  }
}
