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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.sdk.store.descriptor.ScanDescriptor;
import org.apache.carbondata.sdk.store.exception.CarbonException;
import org.apache.carbondata.store.devapi.DataScanner;
import org.apache.carbondata.store.devapi.ResultBatch;
import org.apache.carbondata.store.devapi.ScanUnit;
import org.apache.carbondata.store.impl.service.DataService;
import org.apache.carbondata.store.impl.service.model.ScanRequest;
import org.apache.carbondata.store.impl.service.model.ScanResponse;

/**
 * This scanner scans in local JVM
 * @param <T> scan output
 */
public class RemoteDataScanner<T> implements DataScanner<T> {

  private static final long serialVersionUID = 8373625968314105193L;

  private TableInfo tableInfo;
  private ScanDescriptor scanDescriptor;
  //  private Map<String, String> scanOption;
  private Class<? extends CarbonReadSupport<T>> readSupportClass;

  RemoteDataScanner(TableInfo tableInfo, ScanDescriptor scanDescriptor,
      Map<String, String> scanOption, Class<? extends CarbonReadSupport<T>> readSupportClass) {
    this.tableInfo = tableInfo;
    this.scanDescriptor = scanDescriptor;
    // this.scanOption = scanOption;
    this.readSupportClass = readSupportClass;
  }

  @Override
  public Iterator<? extends ResultBatch<T>> scan(ScanUnit input) throws CarbonException {
    List<CarbonInputSplit> toBeScan = new ArrayList<>();
    if (input instanceof BlockScanUnit) {
      toBeScan.add(((BlockScanUnit) input).getInputSplit());
    } else {
      throw new CarbonException(input.getClass().getName() + " is not supported");
    }
    int queryId = ThreadLocalRandom.current().nextInt();
    CarbonMultiBlockSplit split = new CarbonMultiBlockSplit(toBeScan, input.preferredLocations());
    try {
      ScanRequest request = new ScanRequest(queryId, split, tableInfo,
          scanDescriptor.getProjection(), scanDescriptor.getFilter(), scanDescriptor.getLimit());
      DataService dataService =
          DataServicePool.getOrCreateDataService(((BlockScanUnit) input).getSchedulable());
      ScanResponse response = dataService.scan(request);
      CarbonReadSupport<T> readSupport = readSupportClass.newInstance();
      List<T> rows = Arrays.stream(response.getRows())
          .map(readSupport::readRow)
          .collect(Collectors.toList());

      return Collections.singletonList(new RowMajorResultBatch<>(rows)).iterator();
    } catch (IOException | IllegalAccessException | InstantiationException e) {
      throw new CarbonException(e);
    }
  }
}
