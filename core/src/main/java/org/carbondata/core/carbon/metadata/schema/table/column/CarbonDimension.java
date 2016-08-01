/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.carbon.metadata.schema.table.column;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.encoder.Encoding;

public class CarbonDimension extends CarbonColumn {
  /**
   * serialization version
   */
  private static final long serialVersionUID = 3648269871656322681L;

  /**
   * List of child dimension for complex type
   */
  private List<CarbonDimension> listOfChildDimensions;

  /**
   * in case of dictionary dimension this will store the ordinal
   * of the dimension in mdkey
   */
  private int keyOrdinal;

  /**
   * column group column ordinal
   * for example if column is second column in the group
   * it will store 2
   */
  private int columnGroupOrdinal;

  /**
   * to store complex type dimension ordinal
   */
  private int complexTypeOrdinal;

  public CarbonDimension(ColumnSchema columnSchema, int ordinal, int keyOrdinal,
      int columnGroupOrdinal, int complexTypeOrdinal) {
    super(columnSchema, ordinal);
    this.keyOrdinal = keyOrdinal;
    this.columnGroupOrdinal = columnGroupOrdinal;
    this.complexTypeOrdinal = complexTypeOrdinal;
  }

  /**
   * this method will initialize list based on number of child dimensions Count
   */
  public void initializeChildDimensionsList(int childDimension) {
    listOfChildDimensions = new ArrayList<CarbonDimension>(childDimension);
  }

  /**
   * @return number of children for complex type
   */
  public int getNumberOfChild() {
    return columnSchema.getNumberOfChild();
  }

  /**
   * @return list of children dims for complex type
   */
  public List<CarbonDimension> getListOfChildDimensions() {
    return listOfChildDimensions;
  }

  /**
   * @return return the number of child present in case of complex type
   */
  public int numberOfChild() {
    return columnSchema.getNumberOfChild();
  }

  public boolean hasEncoding(Encoding encoding) {
    return columnSchema.getEncodingList().contains(encoding);
  }

  /**
   * @return the keyOrdinal
   */
  public int getKeyOrdinal() {
    return keyOrdinal;
  }

  /**
   * @return the columnGroupOrdinal
   */
  public int getColumnGroupOrdinal() {
    return columnGroupOrdinal;
  }

  /**
   * @return the complexTypeOrdinal
   */
  public int getComplexTypeOrdinal() {
    return complexTypeOrdinal;
  }

  public void setComplexTypeOridnal(int complexTypeOrdinal) {
    this.complexTypeOrdinal = complexTypeOrdinal;
  }

  /**
   * to generate the hash code for this class
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnSchema == null) ? 0 : columnSchema.hashCode());
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
    if (!(obj instanceof CarbonDimension)) {
      return false;
    }
    CarbonDimension other = (CarbonDimension) obj;
    if (columnSchema == null) {
      if (other.columnSchema != null) {
        return false;
      }
    } else if (!columnSchema.equals(other.columnSchema)) {
      return false;
    }
    return true;
  }
}
