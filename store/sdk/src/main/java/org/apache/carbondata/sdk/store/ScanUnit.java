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

package org.apache.carbondata.sdk.store;

import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;

/**
 * An unit for the scanner in Carbon Store
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public interface ScanUnit<T> extends Serializable {

  /**
   * Return the list of preferred location of this ScanUnit.
   * The default return value is empty string array, which means this ScanUnit
   * has no location preference.
   */
  default String[] preferredLocations() {
    return new String[0];
  }
}
