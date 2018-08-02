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

package org.apache.carbondata.sdk.store.descriptor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

import org.apache.hadoop.io.Writable;

@InterfaceAudience.User
@InterfaceStability.Evolving
public class LoadDescriptor implements Writable {

  private TableIdentifier table;
  private String inputPath;
  private Map<String, String> options;
  private boolean isOverwrite;

  private LoadDescriptor() {
  }

  public LoadDescriptor(TableIdentifier table, String inputPath,
      Map<String, String> options, boolean isOverwrite) {
    Objects.requireNonNull(table);
    Objects.requireNonNull(inputPath);
    this.table = table;
    this.inputPath = inputPath;
    this.options = options;
    this.isOverwrite = isOverwrite;
  }

  public TableIdentifier getTable() {
    return table;
  }

  public void setTable(TableIdentifier table) {
    this.table = table;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }

  public boolean isOverwrite() {
    return isOverwrite;
  }

  public void setOverwrite(boolean overwrite) {
    isOverwrite = overwrite;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    table.write(out);
    out.writeUTF(inputPath);
    out.writeInt(options.size());
    for (Map.Entry<String, String> entry : options.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
    out.writeBoolean(isOverwrite);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    table = new TableIdentifier();
    table.readFields(in);
    inputPath = in.readUTF();
    int size = in.readInt();
    options = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      options.put(in.readUTF(), in.readUTF());
    }
    isOverwrite = in.readBoolean();
  }

  public static class Builder {
    private LoadDescriptor load;
    private Map<String, String> options;

    private Builder() {
      load = new LoadDescriptor();
      options = new HashMap<>();
    }

    public Builder table(TableIdentifier tableIdentifier) {
      load.setTable(tableIdentifier);
      return this;
    }

    public Builder overwrite(boolean isOverwrite) {
      load.setOverwrite(isOverwrite);
      return this;
    }

    public Builder inputPath(String inputPath) {
      load.setInputPath(inputPath);
      return this;
    }

    public Builder options(String key, String value) {
      options.put(key, value);
      return this;
    }

    public LoadDescriptor create() {
      load.setOptions(options);
      return load;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
