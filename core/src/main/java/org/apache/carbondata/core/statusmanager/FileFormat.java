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

package org.apache.carbondata.core.statusmanager;

/**
 * The data file format supported in carbondata project
 */
public enum FileFormat {

  // carbondata columnar file format, optimized for read
  COLUMNAR_V3,

  // carbondata row file format, optimized for write
  ROW_V1;

  public static FileFormat getByOrdinal(int ordinal) {
    if (ordinal < 0 || ordinal >= FileFormat.values().length) {
      return COLUMNAR_V3;
    }

    switch (ordinal) {
      case 0:
        return COLUMNAR_V3;
      case 1:
        return ROW_V1;
    }

    return COLUMNAR_V3;
  }
}
