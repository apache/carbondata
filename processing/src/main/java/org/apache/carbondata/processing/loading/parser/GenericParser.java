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

/**
 * Parse the data according to implementation, The implementation classes can be struct, array or
 * map datatypes.
 * It remains thread safe as the state of implementation class should not change while
 * calling @{@link GenericParser#parse(Object)} method
 */
public interface GenericParser<E> {

  /**
   * Parse the data as per the delimiter
   * @param data
   * @return
   */
  E parse(Object data);

  /**
   * This is used in case of hive where the object is already parsed to Object.
   * So this method will just return the corresponding complex object without parsing the data.
   *
   * @param data The data to be parsed.
   * @return rawData
   */
  E parseRaw(Object data);

}
