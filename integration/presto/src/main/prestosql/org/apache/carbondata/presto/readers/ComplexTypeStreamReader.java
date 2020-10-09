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
import java.util.Objects;
import java.util.Optional;

import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.type.*;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;

import org.apache.carbondata.presto.CarbonVectorBatch;
import org.apache.carbondata.presto.ColumnarVectorWrapperDirect;

/**
 * Class to read the complex type Stream [array/struct/map]
 */

public class ComplexTypeStreamReader extends CarbonColumnVectorImpl
    implements PrestoVectorBlockBuilder {

  protected int batchSize;

  protected Type type;
  protected BlockBuilder builder;

  public ComplexTypeStreamReader(int batchSize, StructField field) {
    super(batchSize, field.getDataType());
    this.batchSize = batchSize;
    this.type = getType(field);
    List<CarbonColumnVector> childrenList = new ArrayList<>();
    for (StructField child : field.getChildren()) {
      childrenList.add(new ColumnarVectorWrapperDirect(Objects.requireNonNull(
          CarbonVectorBatch.createDirectStreamReader(this.batchSize, child.getDataType(), child))));
    }
    setChildrenVector(childrenList);
    this.builder = type.createBlockBuilder(null, batchSize);
  }

  Type getType(StructField field) {
    DataType dataType = field.getDataType();
    if (dataType == DataTypes.STRING || dataType == DataTypes.VARCHAR) {
      return VarcharType.VARCHAR;
    } else if (dataType == DataTypes.SHORT) {
      return SmallintType.SMALLINT;
    } else if (dataType == DataTypes.INT) {
      return IntegerType.INTEGER;
    } else if (dataType == DataTypes.LONG) {
      return BigintType.BIGINT;
    } else if (dataType == DataTypes.DOUBLE) {
      return DoubleType.DOUBLE;
    } else if (dataType == DataTypes.FLOAT) {
      return RealType.REAL;
    } else if (dataType == DataTypes.BOOLEAN) {
      return BooleanType.BOOLEAN;
    } else if (dataType == DataTypes.BINARY) {
      return VarbinaryType.VARBINARY;
    } else if (dataType == DataTypes.DATE) {
      return DateType.DATE;
    } else if (dataType == DataTypes.TIMESTAMP) {
      return TimestampType.TIMESTAMP;
    } else if (dataType == DataTypes.BYTE) {
      return TinyintType.TINYINT;
    } else if (DataTypes.isDecimal(dataType)) {
      org.apache.carbondata.core.metadata.datatype.DecimalType decimal =
          (org.apache.carbondata.core.metadata.datatype.DecimalType) dataType;
      return DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
    } else if (DataTypes.isArrayType(dataType)) {
      return new ArrayType(getType(field.getChildren().get(0)));
    } else if (DataTypes.isStructType(dataType)) {
      List<RowType.Field> children = new ArrayList<>();
      for (StructField child : field.getChildren()) {
        children.add(new RowType.Field(Optional.of(child.getFieldName()), getType(child)));
      }
      return RowType.from(children);
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }

  @Override public Block buildBlock() {
    return builder.build();
  }

  @Override public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void putComplexObject(List<Integer> offsetVector) {
    if (type instanceof ArrayType) {
      // build child block
      Block childBlock = buildChildBlock(getChildrenVector().get(0));
      // prepare an offset vector with 0 as initial offset
      int[] offsetVectorArray = new int[offsetVector.size() + 1];
      for (int i = 1; i <= offsetVector.size(); i++) {
        offsetVectorArray[i] = offsetVectorArray[i - 1] + offsetVector.get(i - 1);
      }
      // prepare Array block
      Block arrayBlock = ArrayBlock
          .fromElementBlock(offsetVector.size(), Optional.empty(), offsetVectorArray,
              childBlock);
      for (int position = 0; position < offsetVector.size(); position++) {
        type.writeObject(builder, arrayBlock.getObject(position, Block.class));
      }
      getChildrenVector().get(0).getColumnVector().reset();
    } else {
      // build child blocks
      List<Block> childBlocks = new ArrayList<>(getChildrenVector().size());
      for (CarbonColumnVector child : getChildrenVector()) {
        childBlocks.add(buildChildBlock(child));
      }
      // prepare ROW block
      Block rowBlock = RowBlock
          .fromFieldBlocks(offsetVector.size(), Optional.empty(),
              childBlocks.toArray(new Block[0]));
      for (int position = 0; position < offsetVector.size(); position++) {
        type.writeObject(builder, rowBlock.getObject(position, Block.class));
      }
      for (CarbonColumnVector child : getChildrenVector()) {
        child.getColumnVector().reset();
      }
    }
  }

  private Block buildChildBlock(CarbonColumnVector carbonColumnVector) {
    DataType dataType = carbonColumnVector.getType();
    carbonColumnVector = carbonColumnVector.getColumnVector();
    if (dataType == DataTypes.STRING || dataType == DataTypes.BINARY
        || dataType == DataTypes.VARCHAR) {
      return ((SliceStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.SHORT) {
      return ((ShortStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.INT || dataType == DataTypes.DATE) {
      return ((IntegerStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.LONG) {
      return ((LongStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.DOUBLE) {
      return ((DoubleStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.FLOAT) {
      return ((FloatStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.TIMESTAMP) {
      return ((TimestampStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.BOOLEAN) {
      return ((BooleanStreamReader) carbonColumnVector).buildBlock();
    } else if (DataTypes.isDecimal(dataType)) {
      return ((DecimalSliceStreamReader) carbonColumnVector).buildBlock();
    } else if (dataType == DataTypes.BYTE) {
      return ((ByteStreamReader) carbonColumnVector).buildBlock();
    } else if (DataTypes.isArrayType(dataType) || (DataTypes.isStructType(dataType))) {
      return ((ComplexTypeStreamReader) carbonColumnVector).buildBlock();
    } else {
      throw new UnsupportedOperationException("unsupported for type :" + dataType);
    }
  }

  @Override public void putNull(int rowId) {
    builder.appendNull();
  }

  @Override public void reset() {
    builder = type.createBlockBuilder(null, batchSize);
  }

  @Override public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      builder.appendNull();
    }
  }
}

