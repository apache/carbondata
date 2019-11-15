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

package org.apache.carbondata.hadoop;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.stream.ExtendedByteArrayOutputStream;
import org.apache.carbondata.core.util.CarbonUtil;

public class CarbonInputSplitWrapper implements Serializable {
  private byte[] data;
  private int size;

  public CarbonInputSplitWrapper(List<CarbonInputSplit> inputSplitList) {
    ExtendedByteArrayOutputStream stream = new ExtendedByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(stream);
    try {
      for (CarbonInputSplit carbonInputSplit : inputSplitList) {
        carbonInputSplit.write(dos);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      CarbonUtil.closeStreams(dos);
    }
    this.data = stream.getBuffer();
    this.size = inputSplitList.size();
  }

  public List<CarbonInputSplit> getInputSplit() {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    DataInputStream dis = new DataInputStream(stream);
    List<CarbonInputSplit> splits = new ArrayList<>();
    try {
      for (int i = 0; i < size; i++) {
        CarbonInputSplit split = new CarbonInputSplit();
        split.readFields(dis);
        splits.add(split);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      CarbonUtil.closeStreams(dis);
    }
    return splits;
  }
}
