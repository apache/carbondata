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

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.Writable;

public class AdaptiveDeltaIntegralCodecMeta extends AdaptiveCodecMeta implements Writable {

  public AdaptiveDeltaIntegralCodecMeta() {
  }

  public AdaptiveDeltaIntegralCodecMeta(DataType targetDataType, SimpleStatsResult stats,
      String compressorName) {
    super(targetDataType, stats, compressorName, Encoding.ADAPTIVE_DELTA_INTEGRAL);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
  }
}