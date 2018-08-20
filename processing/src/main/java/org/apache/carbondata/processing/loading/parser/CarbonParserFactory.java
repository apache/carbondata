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
package org.apache.carbondata.processing.loading.parser;

import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.processing.loading.parser.impl.ArrayParserImpl;
import org.apache.carbondata.processing.loading.parser.impl.PrimitiveParserImpl;
import org.apache.carbondata.processing.loading.parser.impl.StructParserImpl;

public final class CarbonParserFactory {

  /**
   * Create parser for the carbon column.
   *
   * @param carbonColumn
   * @param complexDelimiters
   * @return
   */
  public static GenericParser createParser(CarbonColumn carbonColumn, String[] complexDelimiters,
      String nullFormat) {
    return createParser(carbonColumn, complexDelimiters, nullFormat, 0);
  }

  /**
   * This method may be called recursively if the carbon column is complex type.
   *
   * @param carbonColumn
   * @param complexDelimiters, these delimiters which are used to separate the complex data types.
   * @param depth              It is like depth of tree, if column has children then depth is 1,
   *                           And depth becomes 2 if children has children.
   *                           This depth is used select the complex
   *                           delimiters
   * @return GenericParser
   */
  private static GenericParser createParser(CarbonColumn carbonColumn, String[] complexDelimiters,
      String nullFormat, int depth) {
    DataType dataType = carbonColumn.getDataType();
    if (DataTypes.isArrayType(dataType) || DataTypes.isMapType(dataType)) {
      List<CarbonDimension> listOfChildDimensions =
          ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      // Create array parser with complex delimiter
      ArrayParserImpl arrayParser = new ArrayParserImpl(complexDelimiters[depth], nullFormat);
      for (CarbonDimension dimension : listOfChildDimensions) {
        arrayParser.addChildren(createParser(dimension, complexDelimiters, nullFormat, depth + 1));
      }
      return arrayParser;
    } else if (DataTypes.isStructType(dataType)) {
      List<CarbonDimension> dimensions =
          ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      // Create struct parser with complex delimiter
      StructParserImpl parser = new StructParserImpl(complexDelimiters[depth], nullFormat);
      for (CarbonDimension dimension : dimensions) {
        parser.addChildren(createParser(dimension, complexDelimiters, nullFormat, depth + 1));
      }
      return parser;
    } else {
      return new PrimitiveParserImpl();
    }
  }
}
