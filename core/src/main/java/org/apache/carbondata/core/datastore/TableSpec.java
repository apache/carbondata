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

package org.apache.carbondata.core.datastore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

public class TableSpec {

  // column spec for each dimension and measure
  private DimensionSpec[] dimensionSpec;
  private MeasureSpec[] measureSpec;

  // number of simple dimensions
  private int numSimpleDimensions;

  private CarbonTable carbonTable;

  public TableSpec(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getTableName());
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getTableName());
    // first calculate total number of columnar field considering column group and complex column
    numSimpleDimensions = 0;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.isColumnar()) {
        if (!dimension.isComplex()) {
          numSimpleDimensions++;
        }
      } else {
        throw new UnsupportedOperationException("column group is not supported");
      }
    }
    dimensionSpec = new DimensionSpec[dimensions.size()];
    measureSpec = new MeasureSpec[measures.size()];
    addDimensions(dimensions);
    addMeasures(measures);
  }

  private void addDimensions(List<CarbonDimension> dimensions) {
    int dimIndex = 0;
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (dimension.isColumnar()) {
        if (dimension.isComplex()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.COMPLEX, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.getDataType() == DataTypes.TIMESTAMP && !dimension
            .isDirectDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.isDirectDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.DIRECT_DICTIONARY, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.isGlobalDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(ColumnType.GLOBAL_DICTIONARY, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else {
          DimensionSpec spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension);
          dimensionSpec[dimIndex++] = spec;
        }
      }
    }
  }

  private void addMeasures(List<CarbonMeasure> measures) {
    for (int i = 0; i < measures.size(); i++) {
      CarbonMeasure measure = measures.get(i);
      measureSpec[i] = new MeasureSpec(measure.getColName(), measure.getDataType());
    }
  }

  public DimensionSpec getDimensionSpec(int dimensionIndex) {
    return dimensionSpec[dimensionIndex];
  }

  public MeasureSpec getMeasureSpec(int measureIndex) {
    return measureSpec[measureIndex];
  }

  public int getNumSimpleDimensions() {
    return numSimpleDimensions;
  }

  public int getNumDimensions() {
    return dimensionSpec.length;
  }

  /**
   * return number of measures
   */
  public int getNumMeasures() {
    return measureSpec.length;
  }

  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  public static class ColumnSpec implements Writable {
    // field name of this column
    private String fieldName;

    // data type of this column
    private DataType schemaDataType;

    // dimension type of this dimension
    private ColumnType columnType;

    public ColumnSpec() {
    }

    private ColumnSpec(String fieldName, DataType schemaDataType, ColumnType columnType) {
      this.fieldName = fieldName;
      this.schemaDataType = schemaDataType;
      this.columnType = columnType;
    }

    public static ColumnSpec newInstance(String fieldName, DataType schemaDataType,
        ColumnType columnType) {
      return new ColumnSpec(fieldName, schemaDataType, columnType);
    }

    public static ColumnSpec newInstanceLegacy(String fieldName, DataType schemaDataType,
        ColumnType columnType) {
      // for backward compatibility as the precision and scale is not stored, the values should be
      // initialized with -1 for both precision and scale
      if (schemaDataType instanceof DecimalType) {
        ((DecimalType) schemaDataType).setPrecision(-1);
        ((DecimalType) schemaDataType).setScale(-1);
      }
      return new ColumnSpec(fieldName, schemaDataType, columnType);
    }

    public DataType getSchemaDataType() {
      return schemaDataType;
    }

    public String getFieldName() {
      return fieldName;
    }

    public ColumnType getColumnType() {
      return columnType;
    }

    public int getScale() {
      if (DataTypes.isDecimal(schemaDataType)) {
        return ((DecimalType) schemaDataType).getScale();
      } else if (schemaDataType == DataTypes.BYTE_ARRAY) {
        return -1;
      }
      throw new UnsupportedOperationException();
    }

    public int getPrecision() {
      if (DataTypes.isDecimal(schemaDataType)) {
        return ((DecimalType) schemaDataType).getPrecision();
      } else if (schemaDataType == DataTypes.BYTE_ARRAY) {
        return -1;
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(fieldName);
      out.writeByte(schemaDataType.getId());
      out.writeByte(columnType.ordinal());
      if (DataTypes.isDecimal(schemaDataType)) {
        DecimalType decimalType = (DecimalType) schemaDataType;
        out.writeInt(decimalType.getScale());
        out.writeInt(decimalType.getPrecision());
      } else {
        out.writeInt(-1);
        out.writeInt(-1);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.fieldName = in.readUTF();
      this.schemaDataType = DataTypes.valueOf(in.readByte());
      this.columnType = ColumnType.valueOf(in.readByte());
      int scale = in.readInt();
      int precision = in.readInt();
      if (DataTypes.isDecimal(this.schemaDataType)) {
        DecimalType decimalType = (DecimalType) this.schemaDataType;
        decimalType.setPrecision(precision);
        decimalType.setScale(scale);
      }
    }
  }

  public class DimensionSpec extends ColumnSpec implements Writable {

    // indicate whether this dimension is in sort column
    private boolean inSortColumns;

    // indicate whether this dimension need to do inverted index
    private boolean doInvertedIndex;

    DimensionSpec(ColumnType columnType, CarbonDimension dimension) {
      super(dimension.getColName(), dimension.getDataType(), columnType);
      this.inSortColumns = dimension.isSortColumn();
      this.doInvertedIndex = dimension.isUseInvertedIndex();
    }

    public boolean isInSortColumns() {
      return inSortColumns;
    }

    public boolean isDoInvertedIndex() {
      return doInvertedIndex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }

  public class MeasureSpec extends ColumnSpec implements Writable {

    MeasureSpec(String fieldName, DataType dataType) {
      super(fieldName, dataType, ColumnType.MEASURE);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }
}