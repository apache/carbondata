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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;

/**
 * This instance will be created for implicit column like tupleid.
 */
public class CarbonImplicitDimension extends CarbonDimension {
  /**
   * serialization version
   */
  private static final long serialVersionUID = 3648269871656322681L;

  /**
   * List of encoding that are chained to encode the data for this column
   */
  private List<Encoding> encodingList;

  /**
   *
   * @param ordinal
   */
  private String implicitDimensionName;

  public CarbonImplicitDimension(int ordinal, String implicitDimensionName) {
    super(null, ordinal, -1, -1, -1);
    encodingList = new ArrayList<Encoding>();
    encodingList.add(Encoding.IMPLICIT);
    this.implicitDimensionName = implicitDimensionName;
  }

  public boolean hasEncoding(Encoding encoding) {
    return encodingList.contains(encoding);
  }

  /**
   * @return column unique id
   */
  public String getColumnId() {
    return UUID.randomUUID().toString();
  }

  public Map<String, String> getColumnProperties() {
    return null;
  }

  /**
   * @return if DataType is ARRAY or STRUCT, this method return true, else
   * false.
   */
  public Boolean isComplex() {
    return false;
  }

  /**
   * @return row group id if it is row based
   */
  @Override public int columnGroupId() {
    return -1;
  }

  /**
   * @return the list of encoder used in dimension
   */
  @Override public List<Encoding> getEncoder() {
    return encodingList;
  }

  /**
   * @return return the number of child present in case of complex type
   */
  @Override public int getNumberOfChild() {
    return 0;
  }

  /**
   * @return the colName
   */
  @Override public String getColName() {
    return implicitDimensionName;
  }

  /**
   * @return if column is dimension return true, else false.
   */
  @Override public Boolean isDimension() {
    return true;
  }

  /**
   * @return the dataType
   */
  @Override public DataType getDataType() {
    return DataType.STRING;
  }

  /**
   * @return columnar or row based
   */
  public boolean isColumnar() {
    return true;
  }

  /**
   * To specify the visibily of the column by default its false
   */
  public boolean isInvisible() {
    return true;
  }

  /**
   * to generate the hash code for this class
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((implicitDimensionName == null) ?
        super.hashCode() :
        super.hashCode() + implicitDimensionName.hashCode());
    return result;
  }

  /**
   * to check whether to dimension are equal or not
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof CarbonImplicitDimension)) {
      return false;
    }
    CarbonImplicitDimension other = (CarbonImplicitDimension) obj;
    if (columnSchema == null) {
      if (other.columnSchema != null) {
        return false;
      }
    }
    if (!getColName().equals(other.getColName())) {
      return false;
    }
    return true;
  }
}
