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

package org.apache.carbondata.store.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.sdk.store.conf.StoreConf;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.DataScanner;
import org.apache.carbondata.store.devapi.ResultBatch;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.impl.service.model.ScanRequest;

/**
 * This scanner scans in local JVM
 * @param <T> scan output
 */
public class LocalDataScanner<T> implements DataScanner<T> {

  private static final long serialVersionUID = -983728796814105193L;

  private StoreConf storeConf;
  private ScanDescriptor scanDescriptor;
  private Map<String, String> scanOption;

  LocalDataScanner(StoreConf storeConf, ScanDescriptor scanDescriptor,
      Map<String, String> scanOption) {
    this.storeConf = storeConf;
    this.scanDescriptor = scanDescriptor;
    this.scanOption = scanOption;
  }

  @Override
  public Iterator<? extends ResultBatch<T>> scan(ScanUnit input) throws CarbonException {
    Objects.requireNonNull(scanDescriptor);
    try {
      TableInfo tableInfo = MetaOperation.getTable(scanDescriptor.getTableIdentifier(), storeConf);
      List<CarbonInputSplit> blocks =
          IndexOperation.pruneBlock(tableInfo, scanDescriptor.getFilter());
      CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(blocks, new String[0]);
      ScanRequest scan =
          new ScanRequest(0, split, tableInfo, scanDescriptor.getProjection(),
              scanDescriptor.getFilter(), scanDescriptor.getLimit());
      List<T> rows = (List<T>) DataOperation.scan(tableInfo, scan);
      RowMajorResultBatch<T> resultBatch = new RowMajorResultBatch<>(rows);
      return Collections.singletonList(resultBatch).iterator();
    } catch (IOException e) {
      throw new CarbonException(e);
    }
  }
}
