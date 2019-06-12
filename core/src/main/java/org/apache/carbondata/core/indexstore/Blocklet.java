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
package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

/**
 * Blocklet
 */
public class Blocklet implements Writable,Serializable {

  /** file path of this blocklet */
  private String filePath;

  /** id to identify the blocklet inside the block (it is a sequential number) */
  private String blockletId;

  /**
   * flag to specify whether to consider blocklet Id in equals and hashcode comparison. This is
   * because when CACHE_LEVEL='BLOCK' which is default value, the blocklet ID returned by
   * BlockDataMap pruning will always be -1 and other datamaps will give the the correct blocklet
   * ID. Therefore if we compare -1 with correct blocklet ID the comparison will become wrong and
   * always false will be returned resulting in incorrect result. Default value for flag is true.
   */
  private boolean compareBlockletIdForObjectMatching = true;

  public Blocklet(String filePath, String blockletId) {
    this.filePath = filePath;
    this.blockletId = blockletId;
  }

  public Blocklet(String filePath, String blockletId, boolean compareBlockletIdForObjectMatching) {
    this(filePath, blockletId);
    this.compareBlockletIdForObjectMatching = compareBlockletIdForObjectMatching;
  }

  // For serialization purpose
  public Blocklet() {
  }

  public String getBlockletId() {
    return blockletId;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (filePath == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(filePath);
    }
    if (blockletId == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(blockletId);
    }
    out.writeBoolean(compareBlockletIdForObjectMatching);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      filePath = in.readUTF();
    }
    if (in.readBoolean()) {
      blockletId = in.readUTF();
    }
    this.compareBlockletIdForObjectMatching = in.readBoolean();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Blocklet blocklet = (Blocklet) o;

    if (filePath != null ? !filePath.equals(blocklet.filePath) : blocklet.filePath != null) {
      return false;
    }
    if (!compareBlockletIdForObjectMatching) {
      return true;
    }
    return blockletId != null ?
        blockletId.equals(blocklet.blockletId) :
        blocklet.blockletId == null;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("Blocklet{");
    sb.append("filePath='").append(filePath).append('\'');
    sb.append(", blockletId='").append(blockletId).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public int hashCode() {
    int result = filePath != null ? filePath.hashCode() : 0;
    result = 31 * result;
    if (compareBlockletIdForObjectMatching) {
      result += blockletId != null ? blockletId.hashCode() : 0;
    }
    return result;
  }
}
