/**
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

package org.apache.carbondata.processing.csvreaderstep;

import java.util.HashMap;
import java.util.Map;

public class RddInputUtils {
  private static Map<String, JavaRddIterator<JavaRddIterator<String[]>>> iteratorMap = new
      HashMap<String, JavaRddIterator<JavaRddIterator<String[]>>>();

  public static void put(String key, JavaRddIterator<JavaRddIterator<String[]>> value) {
    iteratorMap.put(key, value);
  }

  public static JavaRddIterator<JavaRddIterator<String[]>> getAndRemove(String key) {
    JavaRddIterator<JavaRddIterator<String[]>> iter = iteratorMap.get(key);
    remove(key);
    return iter;
  }

  public static void remove(String key) {
    iteratorMap.remove(key);
  }
}
