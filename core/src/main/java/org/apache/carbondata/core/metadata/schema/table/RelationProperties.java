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
package org.apache.carbondata.core.metadata.schema.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Class maintain the relation properties of child
 */
public class RelationProperties implements Serializable, Writable {

  /**
   * relation type
   */
  private RelationType relationType;

  /**
   * relation extra properties
   */
  private Map<String, String> properties;

  public RelationProperties(RelationType relationType, Map<String, String> properties) {
    this.relationType = relationType;
    this.properties = properties;
  }

  public RelationType getRelationType() {
    return relationType;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeShort(relationType.ordinal());

    out.writeShort(properties.size());
    Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      out.writeUTF(next.getKey());
      out.writeUTF(next.getValue());
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    int ordinal = in.readShort();
    this.relationType = RelationType.valueOf(ordinal);
    short propertiesSize = in.readShort();
    this.properties = new HashMap<>();
    for (int i = 0; i < propertiesSize; i++) {
      this.properties.put(in.readUTF(), in.readUTF());
    }
  }
}
