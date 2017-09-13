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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

public class TableSpec {

  // column spec for each dimension and measure
  private DimensionSpec[] dimensionSpec;
  private DimensionSpec[] complexDimensionSpec;
  private MeasureSpec[] measureSpec;

  public TableSpec(List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
    addDimensions(dimensions);
    addMeasures(measures);
  }

  private void addDimensions(List<CarbonDimension> dimensions) {
    List<DimensionSpec> dictDims = new ArrayList<>();
    List<DimensionSpec> complexDims = new ArrayList<>();
    for (CarbonDimension dimension : dimensions) {
      if (dimension.isColumnar()) {
        if (dimension.isComplex()) {
          complexDims.add(new DimensionSpec(ColumnType.COMPLEX, dimension));
        } else if (dimension.isDirectDictionaryEncoding()) {
          dictDims.add(new DimensionSpec(ColumnType.DIRECT_DICTIONARY, dimension));
        } else if (dimension.isGlobalDictionaryEncoding()) {
          dictDims.add(new DimensionSpec(ColumnType.GLOBAL_DICTIONARY, dimension));
        } else {
          dictDims.add(new DimensionSpec(ColumnType.PLAIN_VALUE, dimension));
        }
      }
    }
    this.dimensionSpec = dictDims.toArray(new DimensionSpec[dictDims.size()]);
    this.complexDimensionSpec = complexDims.toArray(new DimensionSpec[complexDims.size()]);
  }

  private void addMeasures(List<CarbonMeasure> measures) {
    this.measureSpec = new MeasureSpec[measures.size()];
    for (int i = 0; i < measures.size(); i++) {
      CarbonMeasure measure = measures.get(i);
      measureSpec[i] = new MeasureSpec(measure.getColName(), measure.getDataType(), measure
          .getScale(), measure.getPrecision());
    }
  }

  public DimensionSpec[] getDimensionSpec() {
    return dimensionSpec;
  }

  public DimensionSpec[] getComplexDimensionSpec() {
    return complexDimensionSpec;
  }

  public MeasureSpec[] getMeasureSpec() {
    return measureSpec;
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

    public ColumnSpec(String fieldName, DataType schemaDataType, ColumnType columnType) {
      this.fieldName = fieldName;
      this.schemaDataType = schemaDataType;
      this.columnType = columnType;
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

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(fieldName);
      out.writeByte(schemaDataType.ordinal());
      out.writeByte(columnType.ordinal());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.fieldName = in.readUTF();
      this.schemaDataType = DataType.valueOf(in.readByte());
      this.columnType = ColumnType.valueOf(in.readByte());
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

    private int scale;
    private int precision;

    MeasureSpec(String fieldName, DataType dataType, int scale, int precision) {
      super(fieldName, dataType, ColumnType.MEASURE);
      this.scale = scale;
      this.precision = precision;
    }

    public int getScale() {
      return scale;
    }

    public int getPrecision() {
      return precision;
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