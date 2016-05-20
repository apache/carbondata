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

package org.carbondata.processing.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonDataLoadSchema.DimensionRelation;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;

public final class CarbonSchemaParser {
  /**
   *
   */
  public static final String QUOTES = "\"";

  /**
   * BACK_TICK
   */
  public static final String BACK_TICK = "`";

  private CarbonSchemaParser() {

  }

  /**
   * This method Return the dimension queries based on quotest required or not.
   *
   * @param dimensions
   * @return
   */
  public static String getDimensionSQLQueries(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema, boolean isQuotesRequired, String quote) {
    if (isQuotesRequired) {
      return getDimensionSQLQueriesWithQuotes(dimensions, carbonDataLoadSchema, quote);
    } else {
      return getDimensionSQLQueries(dimensions, carbonDataLoadSchema);
    }
  }

  public static String getDenormColNames(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    //
    List<String> foreignKeys = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    Set<String> allRelationCols = new HashSet<String>();

    for (DimensionRelation dimensionRelation : carbonDataLoadSchema.getDimensionRelationList()) {
      foreignKeys.add(dimensionRelation.getRelation().getFactForeignKeyColumn());
      allRelationCols.addAll(dimensionRelation.getColumns());
    }

    StringBuilder columns = new StringBuilder();

    for (CarbonDimension dim : dimensions) {
      if (foreignKeys.contains(dim.getColName()) && !allRelationCols.contains(dim.getColName())) {
        columns.append(dim.getColName());
        columns.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
    }

    String columnstr = columns.toString();
    if (columnstr.length() > 0 && columnstr.endsWith(CarbonCommonConstants.HASH_SPC_CHARACTER)) {
      columnstr = columnstr
          .substring(0, columnstr.length() - CarbonCommonConstants.HASH_SPC_CHARACTER.length());
    }

    return columnstr;
  }

  /**
   * @param cube
   * @param dimensions
   * @param isQuotesRequired
   * @return
   */
  private static String getDimensionSQLQueries(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    //
    List<String> queryList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (CarbonDimension dim : dimensions) {

      String tableName = extractDimensionTableName(dim.getColName(), carbonDataLoadSchema);
      StringBuilder query;
      String factTableName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
      if (factTableName.equals(tableName)) {
        continue;
      }
      String dimName = dim.getColName();
      query =
          new StringBuilder(dimName + '_' + tableName + CarbonCommonConstants.COLON_SPC_CHARACTER);

      String primaryKey = null;
      for (DimensionRelation dimensionRelation : carbonDataLoadSchema.getDimensionRelationList()) {
        for (String field : dimensionRelation.getColumns()) {
          if (dimName.equals(field)) {
            primaryKey = dimensionRelation.getRelation().getDimensionPrimaryKeyColumn();
            break;
          }
        }
        if (null != primaryKey) {
          break;
        }
      }
      query.append("SELECT ");
      query.append(primaryKey + ',');
      query.append(dimName);
      query.append(" FROM " + tableName);
      queryList.add(query.toString());
    }
    StringBuilder finalQuryString = new StringBuilder();

    for (int i = 0; i < queryList.size() - 1; i++) {
      finalQuryString.append(queryList.get(i));
      finalQuryString.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    if (queryList.size() > 0) {
      finalQuryString.append(queryList.get(queryList.size() - 1));
    }
    return finalQuryString.toString();
  }

  /**
   * @param cube
   * @param dimensions
   * @return
   */

  private static String getDimensionSQLQueriesWithQuotes(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema, String quotes) {
    //
    List<String> queryList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    //        Property[] properties = null;
    for (CarbonDimension dim : dimensions) {

      String tableName = extractDimensionTableName(dim.getColName(), carbonDataLoadSchema);
      StringBuilder query;
      String factTableName = carbonDataLoadSchema.getCarbonTable().getFactTableName();
      if (factTableName.equals(tableName)) {
        continue;
      }
      String dimName = dim.getColName();
      query =
          new StringBuilder(dimName + '_' + tableName + CarbonCommonConstants.COLON_SPC_CHARACTER);

      String primaryKey = null;
      for (DimensionRelation dimensionRelation : carbonDataLoadSchema.getDimensionRelationList()) {
        for (String field : dimensionRelation.getColumns()) {
          if (dimName.equals(field)) {
            primaryKey = dimensionRelation.getRelation().getDimensionPrimaryKeyColumn();
            break;
          }
        }
        if (null != primaryKey) {
          break;
        }
      }
      query.append("SELECT ");
      query.append(quotes + primaryKey + quotes + ',');
      query.append(quotes + dimName + quotes);
      query.append(" FROM " + quotes + tableName + quotes);
      queryList.add(query.toString());
    }
    StringBuilder finalQuryString = new StringBuilder();

    for (int i = 0; i < queryList.size() - 1; i++) {
      finalQuryString.append(queryList.get(i));
      finalQuryString.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    if (queryList.size() > 0) {
      finalQuryString.append(queryList.get(queryList.size() - 1));
    }
    return finalQuryString.toString();
  }

  /**
   * @param dimensions
   * @param measures
   * @param factTableName
   * @param isQuotesRequired
   * @param schemaInfo
   * @return
   */
  public static String getTableInputSQLQuery(List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures, String factTableName, boolean isQuotesRequired,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuilder query = new StringBuilder("SELECT ");

    getQueryForDimension(dimensions, query, factTableName, isQuotesRequired, carbonDataLoadSchema);

    //No properties in new Schema
    //        if (checkIfDenormalized(carbonDataLoadSchema)) {
    //            if (!isQuotesRequired) {
    //                getPropetiesQuerypart(dimensions, query, factTableName, schema);
    //            } else {
    //                getPropetiesQuerypartWithQuotes(dimensions, query, factTableName, schema);
    //            }
    //        }
    if (!"select".equalsIgnoreCase(query.toString().trim())) {
      query.append(",");
    }
    Set<String> uniqueMsrCols = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < measures.size(); i++) {

      uniqueMsrCols.add(measures.get(i).getColName());
    }
    String[] uniqueMeasure = uniqueMsrCols.toArray(new String[uniqueMsrCols.size()]);
    for (int j = 0; j < uniqueMeasure.length; j++) {
      query.append(System.getProperty("line.separator"));
      if (isQuotesRequired) {
        query.append(QUOTES + uniqueMeasure[j] + QUOTES);
      } else {
        query.append(uniqueMeasure[j]);
      }

      if (j != uniqueMeasure.length - 1) {

        query.append(",");
      }
    }
    query.append(System.getProperty("line.separator"));

    if (isQuotesRequired) {
      query.append(" FROM " + QUOTES + factTableName + QUOTES + ' ');
    } else {
      query.append(" FROM " + factTableName + ' ');
    }

    return query.toString();
  }

  /**
   * @param aggDim
   * @param measures
   * @param factTableName
   * @param isQuotesRequired
   * @param schemaInfo
   * @return
   */
  public static String getTableInputSQLQueryForAGG(String[] aggDim, String[] measures,
      String factTableName, boolean isQuotesRequired) {
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    queryBuilder.append(System.getProperty("line.separator"));
    //query.append("\n");
    for (int i = 0; i < aggDim.length; i++) {
      if (isQuotesRequired) {
        queryBuilder.append(QUOTES + aggDim[i] + QUOTES);
      } else {
        queryBuilder.append(aggDim[i]);
      }
      queryBuilder.append(",");
      queryBuilder.append(System.getProperty("line.separator"));
      //query.append("\n");
    }

    for (int i = 0; i < measures.length - 1; i++) {
      if (isQuotesRequired) {
        queryBuilder.append(QUOTES + measures[i] + QUOTES);
      } else {
        queryBuilder.append(measures[i]);
      }
      queryBuilder.append(",");
      queryBuilder.append(System.getProperty("line.separator"));
      //query.append("\n");
    }
    if (isQuotesRequired) {
      queryBuilder.append(QUOTES + measures[measures.length - 1] + QUOTES);
      queryBuilder.append(System.getProperty("line.separator"));
      queryBuilder.append(" FROM " + QUOTES + factTableName + QUOTES);
    } else {
      queryBuilder.append(measures[measures.length - 1]);
      queryBuilder.append(System.getProperty("line.separator"));
      queryBuilder.append(" FROM " + factTableName);
    }
    return queryBuilder.toString();
  }

  private static void getQueryForDimension(List<CarbonDimension> dimensions, StringBuilder query,
      String factTableName, boolean isQuotesRequired, CarbonDataLoadSchema carbonDataLoadSchema) {
    int counter = 0;
    for (CarbonDimension cDim : dimensions) {

      String foreignKey = null;
      for (DimensionRelation dimensionRelation : carbonDataLoadSchema.getDimensionRelationList()) {
        for (String field : dimensionRelation.getColumns()) {
          if (cDim.getColName().equals(field)) {
            foreignKey = dimensionRelation.getRelation().getFactForeignKeyColumn();
          }
        }
      }
      if (foreignKey != null) {
        query.append(System.getProperty("line.separator"));
        if (counter != 0) {
          query.append(',');
        }

        if (isQuotesRequired) {
          query.append(QUOTES + foreignKey + QUOTES);
        } else {
          query.append(foreignKey);
        }
        continue;
      } else {
        query.append(System.getProperty("line.separator"));
        if (counter != 0) {
          query.append(',');
        }

        if (isQuotesRequired) {
          query.append(QUOTES + factTableName + QUOTES + '.' + QUOTES + cDim.getColName() + QUOTES);
        } else {
          query.append(factTableName + '.' + cDim.getColName());
        }
      }
      counter++;
    }
  }

  /**
   * Get dimension string from a array of CubeDimension,which can be shared
   * CubeDimension within schema or in a cube.
   *
   * @param cube
   * @param dimensions
   * @return
   */
  public static int getDimensionString(List<CarbonDimension> dimensions, StringBuilder dimString,
      int counter, CarbonDataLoadSchema carbonDataLoadSchema) {
    for (CarbonDimension cDimension : dimensions) {
      if (!cDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        continue;
      }

      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      dimString.append(
          tableName + '_' + cDimension.getColName() + CarbonCommonConstants.COLON_SPC_CHARACTER
              + counter + CarbonCommonConstants.COLON_SPC_CHARACTER + -1
              + CarbonCommonConstants.COLON_SPC_CHARACTER + 'Y'
              + CarbonCommonConstants.COMA_SPC_CHARACTER);
      counter++;
    }
    return counter;
  }

  /**
   * @param dimensions
   * @param dimString
   * @param counter
   * @param dimCardinalities
   * @return
   */
  public static int getDimensionStringForAgg(String[] dimensions, StringBuilder dimString,
      int counter, Map<String, String> dimCardinalities, String[] acutalDimension) {
    //
    int len = dimensions.length;
    for (int i = 0; i < len - 1; i++) {
      dimString.append(dimensions[i]);
      dimString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
      dimString.append(counter++);
      dimString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
      dimString.append(dimCardinalities.get(acutalDimension[i]));
      dimString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
      dimString.append("Y");
      dimString.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
    }
    //
    dimString.append(dimensions[len - 1]);
    dimString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
    dimString.append(counter++);
    dimString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
    dimString.append(dimCardinalities.get(acutalDimension[len - 1]));
    dimString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
    dimString.append("Y");
    return counter;
  }

  /**
   * Return mapping of Column name to cardinality
   */

  public static Map<String, String> getCardinalities(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    Map<String, String> cardinalities = new LinkedHashMap<String, String>();
    for (CarbonDimension cDimension : dimensions) {
      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      cardinalities.put(tableName + '_' + cDimension.getColName(), -1 + "");
    }
    return cardinalities;
  }

  /**
   * Get measure string from a array of Measure
   *
   * @param measures
   * @return
   */
  public static String getMeasureString(List<CarbonMeasure> measures, int counter) {
    StringBuilder measureString = new StringBuilder();
    int i = measures.size();
    for (CarbonMeasure measure : measures) {

      measureString
          .append(measure.getColName() + CarbonCommonConstants.COLON_SPC_CHARACTER + counter);
      counter++;
      if (i > 1) {
        measureString.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
      }
      i--;

    }
    return measureString.toString();
  }

  /**
   * Get measure string from a array of Measure
   *
   * @param measures
   * @return
   */
  public static String getMeasureStringForAgg(String[] measures, int counter) {
    StringBuilder measureString = new StringBuilder();
    int i = measures.length;
    for (String measure : measures) {

      measureString.append(measure + CarbonCommonConstants.COLON_SPC_CHARACTER + counter);
      counter++;
      if (i > 1) {
        measureString.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
      }
      i--;

    }
    return measureString.toString();
  }

  /**
   * Get measure string from a array of Measure
   *
   * @param measures
   * @return
   */
  public static String getStringWithSeperator(String[] measures, String seperator) {
    StringBuilder measureString = new StringBuilder();
    int i = measures.length;
    for (String measure : measures) {

      measureString.append(measure);
      if (i > 1) {
        measureString.append(seperator);
      }
      i--;

    }
    return measureString.toString();
  }

  /**
   * Get measure string from a array of Measure
   *
   * @param measures
   * @return
   */
  public static String[] getMeasures(List<CarbonMeasure> measures) {
    String[] measuresStringArray = new String[measures.size()];

    for (int i = 0; i < measuresStringArray.length; i++) {
      measuresStringArray[i] = measures.get(i).getColName();
    }
    return measuresStringArray;
  }

  //TODO SIMIAN

  /**
   * Get hierarchy string from dimensions
   *
   * @param dimensions
   * @return
   */
  public static String getHierarchyString(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuilder hierString = new StringBuilder();
    String hierStr = "";

    for (CarbonDimension cDimension : dimensions) {
      if (cDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        continue;
      }
      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      String cDimName = cDimension.getColName();
      hierStr = 0 + CarbonCommonConstants.AMPERSAND_SPC_CHARACTER;
      hierStr = cDimName + '_' + tableName + CarbonCommonConstants.COLON_SPC_CHARACTER + hierStr;
      hierString.append(hierStr);
    }

    hierStr = hierString.toString();
    if (hierStr.length() > 0 && hierStr.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      hierStr = hierStr
          .substring(0, hierStr.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return hierStr;
  }

  /**
   * this method will return table columns
   *
   * @param dimensions
   * @param carbonDataLoadSchema
   * @return
   */
  public static String[] getCubeDimensions(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    List<String> list = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (CarbonDimension cDimension : dimensions) {
      // Ignoring the dimensions which are high cardinality dimension
      if (!cDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        continue;
      }
      list.add(extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema) + "_"
          + cDimension.getColName());
    }
    String[] fields = new String[list.size()];
    fields = list.toArray(fields);
    return fields;
  }

  /**
   * This method will extract dimension table name,
   * By default, fact table name will be returned.
   *
   * @param dimensionColName
   * @param carbonDataLoadSchema
   * @return
   */
  private static String extractDimensionTableName(String dimensionColName,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    List<DimensionRelation> dimensionRelationList = carbonDataLoadSchema.getDimensionRelationList();

    for (DimensionRelation dimensionRelation : dimensionRelationList) {
      for (String field : dimensionRelation.getColumns()) {
        if (dimensionColName.equals(field)) {
          return dimensionRelation.getTableName();
        }
      }
    }
    return carbonDataLoadSchema.getCarbonTable().getFactTableName();
  }


  /**
   * It will return all column groups in below format
   * 0,1~2~3,4,5,6~7~8,9
   * groups are
   * ,-> all ordinal with different group id
   * ~-> all ordinal with same group id
   *
   * @param dimensions
   * @return
   */
  public static String getColumnGroups(List<CarbonDimension> dimensions) {
    StringBuffer columnGroups = new StringBuffer();
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (!dimension.hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      columnGroups.append(dimension.getOrdinal());
      if (i < dimensions.size() - 1) {
        int currGroupOrdinal = dimension.columnGroupId();
        int nextGroupOrdinal = dimensions.get(i + 1).columnGroupId();
        if (currGroupOrdinal == nextGroupOrdinal && currGroupOrdinal != -1) {
          columnGroups.append("~");
        } else {
          columnGroups.append(",");
        }
      }

    }
    return columnGroups.toString();
  }


  /**
   * getHeirAndCardinalityString
   *
   * @param dimensions
   * @param schema
   * @return String
   */
  public static String getHeirAndCardinalityString(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuilder builder = new StringBuilder();
    String heirName = null;
    for (CarbonDimension cDimension : dimensions) {
      heirName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      String dimName = cDimension.getColName();
      builder.append(dimName + '_' + heirName + ".hierarchy");
      builder.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
      builder.append(-1);
      builder.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }
    return builder.toString();
  }

  /**
   * @param dimensions
   * @return
   */
  public static String getMetaHeirString(List<CarbonDimension> dimensions, CarbonTable schema) {
    StringBuilder propString = new StringBuilder();
    String tableName = schema.getFactTableName();
    for (CarbonDimension cDimension : dimensions) {
      propString.append(tableName + "_" + cDimension.getColName());
      propString.append(CarbonCommonConstants.HASH_SPC_CHARACTER);

    }
    // Delete the last special character
    String prop = propString.toString();
    if (prop.endsWith(CarbonCommonConstants.HASH_SPC_CHARACTER)) {
      prop = prop.substring(0, prop.length() - CarbonCommonConstants.HASH_SPC_CHARACTER.length());
    }
    return prop;
  }

  public static String getTableNameString(String factTableName, List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuffer stringBuffer = new StringBuffer();

    for (CarbonDimension cDimension : dimensions) {
      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);

      stringBuffer.append(cDimension.getColName() + '_' + cDimension.getColName());
      stringBuffer.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
      stringBuffer.append(tableName);
      stringBuffer.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }
    // Delete the last & character
    String string = stringBuffer.toString();
    if (string.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      string = string
          .substring(0, string.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return string;
  }

  /**
   * This method will concatenate all the column ids for a given list of dimensions
   *
   * @param dimensions
   * @return
   */
  public static String getColumnIdString(List<CarbonDimension> dimensions) {
    StringBuffer stringBuffer = new StringBuffer();
    for (CarbonDimension cDimension : dimensions) {
      if (!cDimension.hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      stringBuffer.append(cDimension.getColumnId());
      stringBuffer.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }
    // Delete the last & character
    String string = stringBuffer.toString();
    if (string.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      string = string
          .substring(0, string.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return string;
  }

  /**
   * @param dimensions
   * @param schema
   * @return
   */
  public static String getMdkeySizeForFact(List<CarbonDimension> dimensions) {
    int[] dims = new int[dimensions.size()];
    for (int i = 0; i < dims.length; i++) {
      dims[i] = -1;
    }
    return KeyGeneratorFactory.getKeyGenerator(dims).getKeySizeInBytes() + "";
  }

  /**
   * @param dimensions
   * @param dimCardinalities
   * @return
   */
  public static String getMdkeySizeForAgg(String[] dimensions,
      Map<String, String> dimCardinalities) {
    int[] dims = new int[dimensions.length];
    for (int i = 0; i < dimensions.length; i++) {
      dims[i] = Integer.parseInt(dimCardinalities.get(dimensions[i]));
    }
    return KeyGeneratorFactory.getKeyGenerator(dims).getKeySizeInBytes() + "";

  }

  /**
   * @param dimensions
   * @param dimCardinalities
   * @return
   */
  public static KeyGenerator getKeyGeneratorForAGG(String[] dimensions,
      Map<String, String> dimCardinalities) {
    int[] dims = new int[dimensions.length];
    for (int i = 0; i < dimensions.length; i++) {
      dims[i] = Integer.parseInt(dimCardinalities.get(dimensions[i]));
    }
    return KeyGeneratorFactory.getKeyGenerator(dims);

  }

  /**
   * @param dimensions
   * @param schema
   * @return
   */
  public static String getHeirAndKeySizeMapForFact(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuffer stringBuffer = new StringBuffer();
    String heirName = null;
    int[] dims = null;
    int keySizeInBytes = 0;
    for (CarbonDimension cDimension : dimensions) {
      String dimName = cDimension.getColName();
      heirName = extractDimensionTableName(dimName, carbonDataLoadSchema);
      dims = new int[] { -1 };
      keySizeInBytes = KeyGeneratorFactory.getKeyGenerator(dims).getKeySizeInBytes();
      stringBuffer.append(dimName + '_' + heirName + CarbonCommonConstants.HIERARCHY_FILE_EXTENSION
          + CarbonCommonConstants.COLON_SPC_CHARACTER + keySizeInBytes
          + CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }
    return stringBuffer.toString();
  }

  /**
   * @param dimensions
   * @return
   */
  public static String getHierarchyStringWithColumnNames(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {

    StringBuilder hierString = new StringBuilder();
    String hierStr = "";

    for (CarbonDimension cDimension : dimensions) {
      if (cDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        continue;
      }
      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      String cDimName = cDimension.getColName();
      hierStr = cDimName + CarbonCommonConstants.AMPERSAND_SPC_CHARACTER;
      hierStr = cDimName + '_' + tableName + CarbonCommonConstants.COLON_SPC_CHARACTER + hierStr;
      hierString.append(hierStr);
    }

    hierStr = hierString.toString();
    if (hierStr.length() > 0 && hierStr.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      hierStr = hierStr
          .substring(0, hierStr.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return hierStr;

  }

  /**
   * Return foreign key array
   *
   * @param dimensions
   * @return
   */
  public static String[] getForeignKeyForTables(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    Set<String> foreignKey = new LinkedHashSet<String>();
    for (CarbonDimension cDimension : dimensions) {

      List<DimensionRelation> dimensionRelationList =
          carbonDataLoadSchema.getDimensionRelationList();

      for (DimensionRelation dimensionRelation : dimensionRelationList) {
        for (String field : dimensionRelation.getColumns()) {
          if (cDimension.getColName().equals(field)) {
            foreignKey.add(dimensionRelation.getRelation().getFactForeignKeyColumn());
          }
        }
      }

    }
    return foreignKey.toArray(new String[foreignKey.size()]);

  }

  /**
   * Return foreign key and respective hierarchy String.
   *
   * @param dimensions
   * @return
   */
  public static String getForeignKeyHierarchyString(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema, String factTable) {
    StringBuilder foreignKeyHierarchyString = new StringBuilder();
    String columns = "";

    for (CarbonDimension cDimension : dimensions) {
      String dimTableName =
          extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      String dimName = cDimension.getColName();

      if (dimTableName.equals(factTable)) {
        continue;
      }

      String foreignKey = null;
      for (DimensionRelation dimensionRelation : carbonDataLoadSchema.getDimensionRelationList()) {
        for (String field : dimensionRelation.getColumns()) {
          if (dimName.equals(field)) {
            foreignKey = dimensionRelation.getRelation().getFactForeignKeyColumn();
            break;
          }
        }

        foreignKeyHierarchyString.append(foreignKey);
        foreignKeyHierarchyString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
        foreignKeyHierarchyString.append(dimName + '_' + dimTableName);
        foreignKeyHierarchyString.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
      }
    }
    columns = foreignKeyHierarchyString.toString();
    if (columns.length() > 0 && columns.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      columns = columns
          .substring(0, columns.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return columns;

  }

  /**
   * Return foreign key and respective hierarchy String.
   *
   * @param dimensions
   * @param factTableName
   * @return
   */
  public static String getForeignKeyAndPrimaryKeyMapString(
      List<DimensionRelation> dimensionRelationList) {
    StringBuilder foreignKeyHierarchyString = new StringBuilder();
    String columns = "";

    for (DimensionRelation dimensionRelation : dimensionRelationList) {
      foreignKeyHierarchyString.append(dimensionRelation.getRelation().getFactForeignKeyColumn());
      foreignKeyHierarchyString.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
      foreignKeyHierarchyString.append(
          dimensionRelation.getTableName() + '_' + dimensionRelation.getRelation()
              .getDimensionPrimaryKeyColumn());
      foreignKeyHierarchyString.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }
    columns = foreignKeyHierarchyString.toString();
    if (columns.length() > 0 && columns.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      columns = columns
          .substring(0, columns.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return columns;

  }

  /**
   * Return foreign key array
   *
   * @param dimensions
   * @return
   */
  public static String getPrimaryKeyString(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuffer primaryKeyStringbuffer = new StringBuffer();
    for (CarbonDimension cDimension : dimensions) {
      String dimTableName =
          extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      String dimName = cDimension.getColName();

      String primaryKey = null;
      if (dimTableName.equals(carbonDataLoadSchema.getCarbonTable().getFactTableName())) {
        dimTableName = dimName;
      } else {
        for (DimensionRelation dimensionRelation : carbonDataLoadSchema
            .getDimensionRelationList()) {
          for (String field : dimensionRelation.getColumns()) {
            if (field.equals(dimName)) {
              primaryKey = dimensionRelation.getRelation().getDimensionPrimaryKeyColumn();
              break;
            }
          }
        }
      }

      primaryKeyStringbuffer.append(dimTableName + '_' + primaryKey);
      primaryKeyStringbuffer.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);

    }

    String primaryKeyString = primaryKeyStringbuffer.toString();

    if (primaryKeyString.length() > 0 && primaryKeyString
        .endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      primaryKeyString = primaryKeyString.substring(0,
          primaryKeyString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }

    return primaryKeyString;
  }

  /**
   * Get Measure Name String
   *
   * @param cube
   * @return
   */
  public static String getMeasuresNamesString(List<CarbonMeasure> measures) {
    StringBuilder measureNames = new StringBuilder();

    for (int i = 0; i < measures.size(); i++) {
      measureNames.append(measures.get(i).getColName());
      measureNames.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }

    String measureNameString = measureNames.toString();

    if (measureNameString.length() > 0 && measureNameString
        .endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      measureNameString = measureNameString.substring(0,
          measureNameString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }

    return measureNameString;
  }

  /**
   * Get Measure Name String
   *
   * @param cube
   * @return
   */
  public static String getMeasuresUniqueColumnNamesString(List<CarbonMeasure> measures) {
    StringBuilder measureNames = new StringBuilder();
    Set<String> set = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < measures.size(); i++) {
      if (!set.contains(measures.get(i).getColName())) {
        set.add(measures.get(i).getColName());
        measureNames.append(measures.get(i).getColName());
        measureNames.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
      }
    }
    String measureNameString = measureNames.toString();
    if (measureNameString.length() > 0 && measureNameString
        .endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      measureNameString = measureNameString.substring(0,
          measureNameString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }
    return measureNameString;
  }

  /**
   * Get Measure Name String
   *
   * @param cube
   * @return
   */
  public static String getMeasuresNamesStringForAgg(String[] measures) {
    StringBuilder measureNames = new StringBuilder();

    for (int i = 0; i < measures.length; i++) {
      measureNames.append(measures[i]);
      measureNames.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }

    String measureNameString = measureNames.toString();

    if (measureNameString.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      measureNameString = measureNameString.substring(0,
          measureNameString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }

    return measureNameString;
  }

  /**
   * Get Measure Aggregator array
   *
   * @param cube
   * @return
   */
  public static String[] getMeasuresAggragatorArray(List<CarbonMeasure> measures) {
    String[] msrAggregators = new String[measures.size()];

    for (int i = 0; i < msrAggregators.length; i++) {
      msrAggregators[i] = "sum";
    }

    return msrAggregators;
  }

  /**
   * @param schemaInfo
   * @param cube
   * @return
   */
  public static String getActualDimensions(List<CarbonDimension> dimensions) {
    StringBuilder actualDim = new StringBuilder();
    for (CarbonDimension cDimension : dimensions) {
      if (!cDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        continue;
      }
      actualDim.append(cDimension.getColName());
      actualDim.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }

    String actualDimString = actualDim.toString();

    if (actualDimString.length() > 0 && actualDimString
        .endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      actualDimString = actualDimString.substring(0,
          actualDimString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }

    return actualDimString;
  }

  /**
   * @param cube
   * @return
   */
  public static String getActualDimensionsForAggregate(String[] columns) {
    //
    StringBuilder actualDim = new StringBuilder();
    for (String column : columns) {
      actualDim.append(column);
      actualDim.append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }

    String actualDimString = actualDim.toString();

    if (actualDimString.endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      actualDimString = actualDimString.substring(0,
          actualDimString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }

    return actualDimString;
  }

  public static String getMeasuresDataType(List<CarbonMeasure> measures) {
    StringBuilder measureDataTypeString = new StringBuilder();

    for (CarbonMeasure measure : measures) {
      measureDataTypeString.append(measure.getDataType())
          .append(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
    }

    String measureTypeString = measureDataTypeString.toString();

    if (measureTypeString.length() > 0 && measureTypeString
        .endsWith(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER)) {
      measureTypeString = measureTypeString.substring(0,
          measureTypeString.length() - CarbonCommonConstants.AMPERSAND_SPC_CHARACTER.length());
    }

    return measureTypeString;

  }

  /**
   * Below method will be used to get the level and its data type string
   *
   * @param dimensions
   * @param schema
   * @param cube
   * @return String
   */
  public static String getLevelAndDataTypeMapString(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuilder dimString = new StringBuilder();
    for (CarbonDimension cDimension : dimensions) {
      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      String levelName = tableName + '_' + cDimension.getColName();
      dimString.append(levelName + CarbonCommonConstants.LEVEL_FILE_EXTENSION
          + CarbonCommonConstants.COLON_SPC_CHARACTER + cDimension.getDataType()
          + CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    return dimString.toString();
  }

  /**
   * Below method will be used to get the complex dimension string
   *
   * @param dimensions
   * @param schema
   * @param cube
   * @return String
   */
  public static String getComplexTypeString(List<CarbonDimension> dimensions) {
    StringBuilder dimString = new StringBuilder();
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (dimension.getDataType().equals(DataType.ARRAY) || dimension.getDataType()
          .equals(DataType.STRUCT)) {
        addAllComplexTypeChildren(dimension, dimString, "");
        dimString.append(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
      }
    }
    return dimString.toString();
  }

  /**
   * This method will return all the child dimensions under complex dimension
   *
   * @param dimension
   * @param dimString
   * @param parent
   */
  private static void addAllComplexTypeChildren(CarbonDimension dimension, StringBuilder dimString,
      String parent) {
    dimString.append(
        dimension.getColName() + CarbonCommonConstants.COLON_SPC_CHARACTER + dimension.getDataType()
            + CarbonCommonConstants.COLON_SPC_CHARACTER + parent
            + CarbonCommonConstants.COLON_SPC_CHARACTER + dimension.getColumnId()
            + CarbonCommonConstants.HASH_SPC_CHARACTER);
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      CarbonDimension childDim = dimension.getListOfChildDimensions().get(i);
      if (childDim.getNumberOfChild() > 0) {
        addAllComplexTypeChildren(childDim, dimString, dimension.getColName());
      } else {
        dimString.append(
            childDim.getColName() + CarbonCommonConstants.COLON_SPC_CHARACTER + childDim
                .getDataType() + CarbonCommonConstants.COLON_SPC_CHARACTER + dimension.getColName()
                + CarbonCommonConstants.COLON_SPC_CHARACTER + childDim.getColumnId()
                + CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
    }
  }

  /**
   * the method returns the String of direct dictionary column index and column DataType
   * separated by COLON_SPC_CHARACTER
   *
   * @param dimensions
   * @return
   */
  public static String getDirectDictionaryColumnString(List<CarbonDimension> dimensions,
      CarbonDataLoadSchema carbonDataLoadSchema) {
    StringBuffer buff = new StringBuffer();
    int counter = 0;
    for (CarbonDimension cDimension : dimensions) {
      if (cDimension.getEncoder().contains(Encoding.DIRECT_DICTIONARY)) {
        buff.append(cDimension.getOrdinal());
        buff.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
        buff.append(cDimension.getDataType());
        buff.append(CarbonCommonConstants.COLON_SPC_CHARACTER);
        counter++;
      }
    }
    return buff.toString();
  }

  /**
   * Get dimension string from a array of CubeDimension,which can be shared
   * CubeDimension within schema or in a cube.
   *
   * @param cube
   * @param dimensions
   * @return
   */
  public static int getNoDictionaryDimensionString(List<CarbonDimension> dimensions,
      StringBuilder dimString, int counter, CarbonDataLoadSchema carbonDataLoadSchema) {
    for (CarbonDimension cDimension : dimensions) {
      if (cDimension.getEncoder().contains(Encoding.DICTIONARY)) {
        continue;
      }

      String tableName = extractDimensionTableName(cDimension.getColName(), carbonDataLoadSchema);
      dimString.append(
          tableName + '_' + cDimension.getColName() + CarbonCommonConstants.COLON_SPC_CHARACTER
              + counter + CarbonCommonConstants.COLON_SPC_CHARACTER + -1
              + CarbonCommonConstants.COLON_SPC_CHARACTER + 'Y'
              + CarbonCommonConstants.COMA_SPC_CHARACTER);
      counter++;
    }
    return counter;
  }
}
