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

import org.apache.carbondata.core.metadata.schema.table.Writable;

/**
 * Blocklet
 */
public class Blocklet implements Writable,Serializable {

  private String blockId;

  private String blockletId;

  public Blocklet(String blockId, String blockletId) {
    this.blockId = blockId;
    this.blockletId = blockletId;
  }

  // For serialization purpose
  public Blocklet() {
  }

  public String getBlockletId() {
    return blockletId;
  }

  public String getBlockId() {
    return blockId;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeUTF(blockId);
    out.writeUTF(blockletId);
  }

  @Override public void readFields(DataInput in) throws IOException {
    blockId = in.readUTF();
    blockletId = in.readUTF();
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Blocklet blocklet = (Blocklet) o;

    if (blockId != null ? !blockId.equals(blocklet.blockId) : blocklet.blockId != null) {
      return false;
    }
    return blockletId != null ?
        blockletId.equals(blocklet.blockletId) :
        blocklet.blockletId == null;
  }

  @Override public int hashCode() {
    int result = blockId != null ? blockId.hashCode() : 0;
    result = 31 * result + (blockletId != null ? blockletId.hashCode() : 0);
    return result;
  }
}
