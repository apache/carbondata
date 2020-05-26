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

package org.apache.carbondata.presto.readers;

import java.util.ArrayList;
import java.util.List;

import io.prestosql.spi.type.*;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;

import org.apache.carbondata.presto.CarbonVectorBatch;

/**
 * Class to read the Array Stream
 */

public class ArrayStreamReader extends CarbonColumnVectorImpl implements PrestoVectorBlockBuilder {

  protected int batchSize;

  protected Type type;
  protected BlockBuilder builder;
  Block childBlock = null;
  private int index = 0;

  public ArrayStreamReader(int batchSize, DataType dataType, StructField field) {
    super(batchSize, dataType);
    this.batchSize = batchSize;
    this.type = getArrayOfType(field, dataType);
    ArrayList<CarbonColumnVectorImpl> childrenList= new ArrayList<>();
    childrenList.add(CarbonVectorBatch.createDirectStreamReader(this.batchSize, field.getDataType(), field));
    setChildrenVector(childrenList);
    this.builder = type.createBlockBuilder(null, batchSize);
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public String getDataTypeName() {
    return "ARRAY";
  }

  Type getArrayOfType(StructField field, DataType dataType) {
    if (dataType == DataTypes.STRING) {
      return new ArrayType(VarcharType.VARCHAR);
    } else if (dataType == DataTypes.BYTE) {
      return new ArrayType(TinyintType.TINYINT);
    } else if (dataType == DataTypes.SHORT) {
      return new ArrayType(SmallintType.SMALLINT);
    } else if (dataType == DataTypes.INT) {
      return new ArrayType(IntegerType.INTEGER);
    } else if (dataType == DataTypes.LONG) {
      return new ArrayType(BigintType.BIGINT);
    } else if (dataType == DataTypes.DOUBLE) {
      return new ArrayType(DoubleType.DOUBLE);
    } else if (dataType == DataTypes.FLOAT) {
      return new ArrayType(RealType.REAL);
    } else if (dataType == DataTypes.BOOLEAN) {
      return new ArrayType(BooleanType.BOOLEAN);
    } else if (dataType == DataTypes.TIMESTAMP) {
      return new ArrayType(TimestampType.TIMESTAMP);
    } else if (DataTypes.isArrayType(dataType)) {
      StructField childField = field.getChildren().get(0);
      return new ArrayType(getArrayOfType(childField, childField.getDataType()));
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }

  @Override
  public Block buildBlock() {
    return builder.build();
  }

  public boolean isComplex() {
    return true;
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public void putObject(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
    } else {
      getChildrenVector().get(0).putObject(rowId, value);
    }
  }

  public void putArrayObject() {
    if (DataTypes.isArrayType(this.getType())) {
      childBlock = ((ArrayStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.STRING) {
      childBlock = ((SliceStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.INT) {
      childBlock = ((IntegerStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.LONG) {
      childBlock = ((LongStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.DOUBLE) {
      childBlock = ((DoubleStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.FLOAT) {
      childBlock = ((FloatStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.BOOLEAN) {
      childBlock = ((BooleanStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.BYTE) {
      childBlock = ((ByteStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.TIMESTAMP) {
      childBlock = ((TimestampStreamReader) getChildrenVector().get(0)).buildBlock();
    } else if (this.getType() == DataTypes.SHORT) {
      childBlock = ((ShortStreamReader) getChildrenVector().get(0)).buildBlock();
    }
    type.writeObject(builder, childBlock);
    getChildrenVector().get(0).reset();
  }

  @Override
  public void putNull(int rowId) {
    builder.appendNull();
  }

  @Override
  public void reset() {
    builder = type.createBlockBuilder(null, batchSize);
  }

  @Override
  public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      builder.appendNull();
    }
  }
}

