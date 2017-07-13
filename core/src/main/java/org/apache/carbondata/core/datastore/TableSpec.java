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

import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

public class TableSpec {

  // column spec for each dimension and measure
  private DimensionSpec[] dimensionSpec;
  private MeasureSpec[] measureSpec;

  // number of simple dimensions
  private int numSimpleDimensions;

  public TableSpec(List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
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
          DimensionSpec spec = new DimensionSpec(DimensionType.COMPLEX, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.getDataType() == DataType.TIMESTAMP ||
            dimension.getDataType() == DataType.DATE) {
          DimensionSpec spec = new DimensionSpec(DimensionType.DIRECT_DICTIONARY, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else if (dimension.isGlobalDictionaryEncoding()) {
          DimensionSpec spec = new DimensionSpec(DimensionType.GLOBAL_DICTIONARY, dimension);
          dimensionSpec[dimIndex++] = spec;
        } else {
          DimensionSpec spec = new DimensionSpec(DimensionType.PLAIN_VALUE, dimension);
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


  public class ColumnSpec {
    // field name of this column
    private String fieldName;

    // data type of this column
    private DataType dataType;

    ColumnSpec(String fieldName, DataType dataType) {
      this.fieldName = fieldName;
      this.dataType = dataType;
    }

    public DataType getDataType() {
      return dataType;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  public class DimensionSpec extends ColumnSpec {

    // dimension type of this dimension
    private DimensionType type;

    // indicate whether this dimension is in sort column
    private boolean inSortColumns;

    // indicate whether this dimension need to do inverted index
    private boolean doInvertedIndex;

    DimensionSpec(DimensionType dimensionType, CarbonDimension dimension) {
      super(dimension.getColName(), dimension.getDataType());
      this.type = dimensionType;
      this.inSortColumns = dimension.isSortColumn();
      this.doInvertedIndex = dimension.isUseInvertedIndex();
    }

    public DimensionType getDimensionType() {
      return type;
    }

    public boolean isInSortColumns() {
      return inSortColumns;
    }

    public boolean isDoInvertedIndex() {
      return doInvertedIndex;
    }
  }

  public class MeasureSpec extends ColumnSpec {

    MeasureSpec(String fieldName, DataType dataType) {
      super(fieldName, dataType);
    }

  }
}