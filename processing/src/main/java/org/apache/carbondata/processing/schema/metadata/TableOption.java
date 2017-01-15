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

package org.apache.carbondata.processing.schema.metadata;

/**
 * This class is to hold the key value pair of properties needed while dataload.
 */
public class TableOption {
  /**
   * option key name
   */
  private String optionKey;
  /**
   * option key value
   */
  private String optionValue;

  /**
   * the constructor to initialize the key value pair TableOption instance
   *
   * @param optionKey
   * @param optionValue
   */
  public TableOption(String optionKey, String optionValue) {
    this.optionKey = optionKey;
    this.optionValue = optionValue;
  }

  /**
   * constructor to init from te string separated by comma(,)
   *
   * @param str
   */
  public TableOption(String str) {
    //passing 2 to split the key value pair having empty value for the corresponding key.
    String[] split = str.split(",", 2);
    this.optionKey = split[0];
    this.optionValue = split[1];
  }

  /**
   * returns options key
   *
   * @return
   */
  public String getOptionKey() {
    return optionKey;
  }

  /**
   * returns options value
   *
   * @return
   */
  public String getOptionValue() {
    return optionValue;
  }

  /**
   * @return
   */
  public String toString() {
    return optionKey + "," + optionValue;
  }
}
