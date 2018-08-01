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

package org.apache.carbondata.store.impl.master;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.sdk.store.ScanUnit;
import org.apache.carbondata.store.impl.BlockScanUnit;
import org.apache.carbondata.store.impl.rpc.PruneService;
import org.apache.carbondata.store.impl.rpc.model.PruneRequest;
import org.apache.carbondata.store.impl.rpc.model.PruneResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

public class PruneServiceImpl implements PruneService {

  @Override
  public PruneResponse prune(PruneRequest request) throws IOException {
    Configuration hadoopConf = request.getHadoopConf();
    Job job = new Job(hadoopConf);
    CarbonTableInputFormat format = new CarbonTableInputFormat(hadoopConf);
    List<InputSplit> prunedResult = format.getSplits(job);

    List<ScanUnit> output = prunedResult.stream().map(
        (Function<InputSplit, ScanUnit>) inputSplit ->
            new BlockScanUnit((CarbonInputSplit) inputSplit)
    ).collect(Collectors.toList());
    return new PruneResponse(output);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException {
    return null;
  }
}
