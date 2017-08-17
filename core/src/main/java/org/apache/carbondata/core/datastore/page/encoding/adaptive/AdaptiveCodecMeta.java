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

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodecMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.Writable;

/**
 * Metadata for AdaptiveIntegralCodec and DeltaIntegralCodec
 */
public class AdaptiveCodecMeta extends ColumnPageCodecMeta implements Writable {

  private DataType targetDataType;
  private String compressorName;

  public AdaptiveCodecMeta() {

  }

  public AdaptiveCodecMeta(DataType targetDataType, SimpleStatsResult stats,
      String compressorName, Encoding encoding) {
    super(stats.getDataType(), encoding, stats);
    this.targetDataType = targetDataType;
    this.compressorName = compressorName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(targetDataType.ordinal());
    out.writeUTF(compressorName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.targetDataType = DataType.valueOf(in.readByte());
    this.compressorName = in.readUTF();
  }

  public DataType getTargetDataType() {
    return targetDataType;
  }

  public String getCompressorName() {
    return compressorName;
  }
}
