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

package org.apache.carbondata.core.metadata.schema.table.column;

/**
 * class represent column(measure) in table
 */
public class CarbonMeasure extends CarbonColumn {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 354341488059013977L;

  public CarbonMeasure(ColumnSchema columnSchema, int ordinal) {
    this(columnSchema, ordinal, 0);
  }

  public CarbonMeasure(ColumnSchema columnSchema, int ordinal, int schemaOrdinal) {
    super(columnSchema, ordinal, schemaOrdinal);
  }

  /**
   * @return the scale
   */
  public int getScale() {
    return columnSchema.getScale();
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return columnSchema.getPrecision();
  }

  /**
   * to check whether to dimension are equal or not
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof CarbonMeasure)) {
      return false;
    }
    CarbonMeasure other = (CarbonMeasure) obj;
    if (columnSchema == null) {
      if (other.columnSchema != null) {
        return false;
      }
    } else if (!columnSchema.equals(other.columnSchema)) {
      return false;
    }
    return true;
  }

  /**
   * hash code
   * @return
   */
  @Override
  public int hashCode() {
    return this.getColumnSchema().getColumnUniqueId().hashCode();
  }
}
