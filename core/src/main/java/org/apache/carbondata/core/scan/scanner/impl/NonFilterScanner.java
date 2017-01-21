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
package org.apache.carbondata.core.scan.scanner.impl;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedResult;
import org.apache.carbondata.core.scan.scanner.AbstractBlockletScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Non filter processor which will be used for non filter query
 * In case of non filter query we just need to read all the blocks requested in the
 * query and pass it to scanned result
 */
public class NonFilterScanner extends AbstractBlockletScanner {

  public NonFilterScanner(BlockExecutionInfo blockExecutionInfo,
                          QueryStatisticsModel queryStatisticsModel) {
    super(blockExecutionInfo);
    // as its a non filter query creating a non filter query scanned result object
    scannedResult = new NonFilterQueryScannedResult(blockExecutionInfo);
    super.queryStatisticsModel = queryStatisticsModel;
  }
}
