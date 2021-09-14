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

package org.apache.carbondata.core.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Abstract class for Custom Index implementation. When index property is configured on table, a
 * new column is created within carbon layer from the set of schema columns in the table.
 * An Index implementation class must extend this class and provide the concrete implementation
 * for following abstract methods:
 * 1. Init method to extract and store the sub-properties of index property. Such as index type,
 *    source columns etc.
 * 2. Generate method to generate the row value for the index column from corresponding row values
 *    of its source columns.
 * 3. Query method to process the custom UDF filter queries based on source columns.
 * @param <ReturnType>
 */
public abstract class CustomIndex<ReturnType> implements Serializable {

  private static final long serialVersionUID = 6529685098267757692L;
  /**
   * Initialize the custom index instance.
   * @param indexName
   * @param properties
   * @throws Exception
   */
  public abstract void init(String indexName, Map<String, String> properties) throws Exception;

  /**
   * Generates the custom index column value from the given source columns.
   * @param columns
   * @return Returns generated column value
   * @throws Exception
   */
  public abstract String generate(List<?> columns) throws Exception;

  /**
   * Query processor for custom index.
   * @param query
   * @return Returns list of ranges to be fetched
   * @throws Exception
   */
  public abstract ReturnType query(String query) throws Exception;

  /**
   * Query processor for custom index.
   * @param queryPointList query point list for GeoHashIndex
   * @return Returns list of ranges to be fetched
   * @throws Exception
   */
  public abstract ReturnType query(List<double[]> queryPointList) throws Exception;

  /**
   * Deserializes and returns the custom index instance
   * @param serializedInstance
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static CustomIndex getCustomInstance(String serializedInstance) throws IOException {
    return (CustomIndex) ObjectSerializationUtil.convertStringToObject(serializedInstance);
  }

  /**
   * Serializes the custom index instance
   * @param instance
   * @return
   * @throws IOException
   */
  public static String getCustomInstance(CustomIndex instance) throws IOException {
    return ObjectSerializationUtil.convertObjectToString(instance);
  }
}
