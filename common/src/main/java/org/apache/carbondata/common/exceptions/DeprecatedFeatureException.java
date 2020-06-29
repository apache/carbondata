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

package org.apache.carbondata.common.exceptions;

public class DeprecatedFeatureException extends RuntimeException {

  private DeprecatedFeatureException(String featureName) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3606
    super(featureName + " is deprecated in CarbonData 2.0");
  }

  public static void globalDictNotSupported() throws DeprecatedFeatureException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    throw new DeprecatedFeatureException("Global dictionary");
  }

  public static void customPartitionNotSupported() throws DeprecatedFeatureException {
    throw new DeprecatedFeatureException("Custom partition");
  }

}
