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

import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * It just have 2 types right now, either fixed or variable.
 */
public abstract class CarbonRowSchema {

  protected DataType dataType;

  public CarbonRowSchema(DataType dataType) {
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
  public abstract int getLength();

  /**
   * schema type
   * @return
   */
  public abstract DataMapSchemaType getSchemaType();

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

    @Override public int getLength() {
      if (length == 0) {
        return dataType.getSizeInBytes();
      } else {
        return length;
      }
    }

    @Override public DataMapSchemaType getSchemaType() {
      return DataMapSchemaType.FIXED;
    }
  }

  public static class VariableCarbonRowSchema extends CarbonRowSchema {

    public VariableCarbonRowSchema(DataType dataType) {
      super(dataType);
    }

    @Override public int getLength() {
      return dataType.getSizeInBytes();
    }

    @Override public DataMapSchemaType getSchemaType() {
      return DataMapSchemaType.VARIABLE;
    }
  }

  public static class StructCarbonRowSchema extends CarbonRowSchema {

    private CarbonRowSchema[] childSchemas;

    public StructCarbonRowSchema(DataType dataType, CarbonRowSchema[] childSchemas) {
      super(dataType);
      this.childSchemas = childSchemas;
    }

    @Override public int getLength() {
      return dataType.getSizeInBytes();
    }

    public CarbonRowSchema[] getChildSchemas() {
      return childSchemas;
    }

    @Override public DataMapSchemaType getSchemaType() {
      return DataMapSchemaType.STRUCT;
    }
  }

  public enum DataMapSchemaType {
    FIXED, VARIABLE, STRUCT
  }
}
