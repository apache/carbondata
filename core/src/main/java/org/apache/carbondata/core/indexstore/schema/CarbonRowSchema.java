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

package org.apache.carbondata.core.indexstore.schema;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * It just have 2 types right now, either fixed or variable.
 */
public abstract class CarbonRowSchema implements Serializable {

  private static final long serialVersionUID = -8061282029097686495L;

  protected DataType dataType;
  private int bytePosition = -1;

  public CarbonRowSchema(DataType dataType) {
    this.dataType = dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  /**
   * Either fixed or variable length.
   *
   * @return
   */
  public DataType getDataType() {
    return dataType;
  }

  /**
   * Gives length in case of fixed schema other wise returns length
   *
   * @return
   */
  public int getLength() {
    return dataType.getSizeInBytes();
  }

  public void setBytePosition(int bytePosition) {
    this.bytePosition = bytePosition;
  }

  public int getBytePosition() {
    return this.bytePosition;
  }

  /**
   * schema type
   * @return
   */
  public abstract IndexSchemaType getSchemaType();

  /*
 * It has always fixed length, length cannot be updated later.
 * Usage examples : all primitive types like short, int etc
 */
  public static class FixedCarbonRowSchema extends CarbonRowSchema {

    private int length;

    public FixedCarbonRowSchema(DataType dataType) {
      super(dataType);
    }

    public FixedCarbonRowSchema(DataType dataType, int length) {
      super(dataType);
      this.length = length;
    }

    @Override
    public int getLength() {
      if (length == 0) {
        return dataType.getSizeInBytes();
      } else {
        return length;
      }
    }

    @Override
    public IndexSchemaType getSchemaType() {
      return IndexSchemaType.FIXED;
    }
  }

  public static class VariableCarbonRowSchema extends CarbonRowSchema {
    private boolean isVarcharType = false;

    public VariableCarbonRowSchema(DataType dataType) {
      super(dataType);
    }

    public VariableCarbonRowSchema(DataType dataType, boolean isVarcharType) {
      super(dataType);
      this.isVarcharType = isVarcharType;
    }

    @Override
    public int getLength() {
      return dataType.getSizeInBytes();
    }

    @Override
    public IndexSchemaType getSchemaType() {
      return isVarcharType ? IndexSchemaType.VARIABLE_INT : IndexSchemaType.VARIABLE_SHORT;
    }
  }

  public static class StructCarbonRowSchema extends CarbonRowSchema {

    private CarbonRowSchema[] childSchemas;

    public StructCarbonRowSchema(DataType dataType, CarbonRowSchema[] childSchemas) {
      super(dataType);
      this.childSchemas = childSchemas;
    }

    @Override
    public int getLength() {
      return dataType.getSizeInBytes();
    }

    public CarbonRowSchema[] getChildSchemas() {
      return childSchemas;
    }

    @Override
    public IndexSchemaType getSchemaType() {
      return IndexSchemaType.STRUCT;
    }
  }

  public enum IndexSchemaType {
    FIXED, VARIABLE_INT, VARIABLE_SHORT, STRUCT
  }
}
