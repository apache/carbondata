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

package org.apache.carbondata.core.metadata.datatype;

import java.util.List;

public class StructType extends DataType {

  private List<StructField> fields;

  public StructType(List<StructField> fields) {
    super(DataTypes.STRUCT_TYPE_ID, 10, "STRUCT", -1);
    this.fields = fields;
  }

  @Override
  public boolean isComplexType() {
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    StructType other = (StructType) obj;
    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (!this.getFields().equals(other.getFields())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getName().hashCode();
    result = prime * result + getFields().hashCode();
    return result;
  }

  public List<StructField> getFields() {
    return fields;
  }
}
