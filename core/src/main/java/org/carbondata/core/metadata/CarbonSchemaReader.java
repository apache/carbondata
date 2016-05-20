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

package org.carbondata.core.metadata;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.core.carbon.CarbonDef.CubeDimension;
import org.carbondata.core.carbon.CarbonDef.Dimension;
import org.carbondata.core.carbon.CarbonDef.DimensionUsage;
import org.carbondata.core.carbon.CarbonDef.Hierarchy;
import org.carbondata.core.carbon.CarbonDef.Level;
import org.carbondata.core.carbon.CarbonDef.Measure;
import org.carbondata.core.carbon.CarbonDef.RelationOrJoin;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.CarbonDef.Table;
import org.carbondata.core.constants.CarbonCommonConstants;

public final class CarbonSchemaReader {

  private CarbonSchemaReader() {

  }

  /**
   * getMondrianCube
   *
   * @param schema
   * @param cubeName
   * @return Cube
   */
  public static Cube getMondrianCube(Schema schema, String cubeName) {
    Cube[] cubes = schema.cubes;
    for (Cube cube : cubes) {
      String cubeUniqueName = schema.name + '_' + cube.name;
      if (cubeUniqueName.equals(cubeName)) {
        return cube;
      }
    }
    return null;
  }

  /**
   * Get measure string from a array of Measure
   */
  public static String[] getMeasures(Measure[] measures) {
    String[] measuresStringArray = new String[measures.length];

    for (int i = 0; i < measuresStringArray.length; i++) {
      measuresStringArray[i] = measures[i].column;
    }
    return measuresStringArray;
  }

  /**
   * Get the name of a fact table in a cube
   */
  public static String getFactTableName(Cube cube) {
    Table factTable = (Table) cube.fact;
    return factTable.name;
  }


  public static String[] getDimensions(Cube cube) {
    //
    String factTable = ((Table) cube.fact).name;
    List<String> list = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    CubeDimension[] dimensions = cube.dimensions;
    for (CubeDimension cDimension : dimensions) {
      for (Hierarchy hierarchy : ((Dimension) cDimension).hierarchies) {
        RelationOrJoin relation = hierarchy.relation;
        String tableName = relation == null ? factTable : ((Table) hierarchy.relation).name;
        for (Level level : hierarchy.levels) {
          list.add(tableName + '_' + level.column);
        }
      }
    }
    String[] fields = new String[list.size()];
    fields = list.toArray(fields);
    return fields;
  }

  /**
   * Extracts the hierarchy from Dimension or Dimension usage(basedon multiple cubes)
   */
  public static Hierarchy[] extractHierarchies(Schema schemaInfo, CubeDimension cDimension) {
    Hierarchy[] hierarchies = null;
    if (cDimension instanceof Dimension) {
      hierarchies = ((Dimension) cDimension).hierarchies;
    } else if (cDimension instanceof DimensionUsage) {
      String sourceDimensionName = ((DimensionUsage) cDimension).source;
      Dimension[] schemaGlobalDimensions = schemaInfo.dimensions;
      for (Dimension dimension : schemaGlobalDimensions) {
        if (sourceDimensionName.equals(dimension.name)) {
          hierarchies = dimension.hierarchies;
        }
      }
    }
    return hierarchies;
  }
}
