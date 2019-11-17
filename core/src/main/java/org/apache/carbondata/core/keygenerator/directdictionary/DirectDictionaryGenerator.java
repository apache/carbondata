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

package org.apache.carbondata.core.keygenerator.directdictionary;

import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * The interface provides the method to generate dictionary key
 * and getting the actual value from the dictionaryKey for direct dictionary column.
 */
public interface DirectDictionaryGenerator {

  /**
   * The method generate and returns the dictionary / surrogate key for direct dictionary column
   *
   * @param member The member string value
   * @return returns dictionary/ surrogate value
   */
  int generateDirectSurrogateKey(String member);

  /**
   * The method returns the actual value of the requested dictionary / surrogate
   *
   * @param key
   * @return dictionary actual member
   */
  Object getValueFromSurrogate(int key);

  int generateKey(long value);

  /**
   * The method generate and returns the dictionary / surrogate key for direct dictionary column
   * This Method is called while executing filter queries for getting direct surrogate members.
   * Currently the query engine layer only supports yyyy-MM-dd HH:mm:ss date format no matter
   * in which format the data is been stored, so while retrieving the direct surrogate value for
   * filter member first it should be converted in date form as per above format and needs to
   * retrieve time stamp.
   *
   * @param memberStr The member string value
   * @return returns dictionary/ surrogate value
   */
  int generateDirectSurrogateKey(String memberStr, String format);

  void initialize();

  /**
   * Return value datatype for this generator
   *
   * @return
   */
  DataType getReturnType();

}
