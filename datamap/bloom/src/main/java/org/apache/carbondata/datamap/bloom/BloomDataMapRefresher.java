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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapRefresher;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

public class BloomDataMapRefresher extends BloomDataMapWriter implements DataMapRefresher {

  BloomDataMapRefresher(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, int bloomFilterSize) throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName, bloomFilterSize);
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
    super.addRow(values);
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
