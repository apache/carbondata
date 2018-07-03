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

package org.apache.carbondata.store.rest.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.store.exception.StoreException;

import org.apache.commons.lang.StringUtils;

public class ServiceUtil {

  public static Expression parseFilter(String filter) {
    if (filter == null) {
      return null;
    }

    // TODO parse filter sql to Expression object

    return null;
  }

  public static List<String> prepareSortColumns(
      Field[] fields, Map<String, String> properties) throws StoreException {

    List<String> sortColumnsList = new ArrayList<>();
    Set<Map.Entry<String, String>> entries = properties.entrySet();
    String sortKeyString = null;
    for (Map.Entry<String, String> entry : entries) {
      if (CarbonCommonConstants.SORT_COLUMNS.equalsIgnoreCase(entry.getKey())) {
        sortKeyString = CarbonUtil.unquoteChar(entry.getValue()).trim();
      }
    }

    if (sortKeyString != null) {
      String[] sortKeys = sortKeyString.split(",", -1);
      for (int i = 0; i < sortKeys.length; i++) {
        sortKeys[i] = sortKeys[i].trim().toLowerCase();
        if (StringUtils.isEmpty(sortKeys[i])) {
          throw new StoreException("SORT_COLUMNS contains illegal argument.");
        }
      }

      for (int i = sortKeys.length - 2; i >= 0; i--) {
        for (int j = i + 1; j < sortKeys.length; j++) {
          if (sortKeys[i].equals(sortKeys[j])) {
            throw new StoreException(
                "SORT_COLUMNS Either having duplicate columns : " + sortKeys[i]);
          }
        }
      }

      for (int i = sortKeys.length - 1; i >= 0; i--) {
        boolean isExists = false;
        for (int j = fields.length - 1; j >= 0; j--) {
          if (sortKeys[i].equalsIgnoreCase(fields[j].getFieldName())) {
            sortKeys[i] = fields[j].getFieldName();
            isExists = true;
            break;
          }
        }
        if (!isExists) {
          String message = "sort_columns: " + sortKeys[i]
              + " does not exist in table. Please check create table statement.";
          throw new StoreException(message);
        }
      }
      sortColumnsList = Arrays.asList(sortKeys);
    } else {
      for (Field field : fields) {
        if (null != field) {
          if (field.getDataType() == DataTypes.STRING ||
              field.getDataType() == DataTypes.DATE  ||
              field.getDataType() == DataTypes.TIMESTAMP) {
            sortColumnsList.add(field.getFieldName());
          }
        }
      }
    }
    return sortColumnsList;
  }

}
