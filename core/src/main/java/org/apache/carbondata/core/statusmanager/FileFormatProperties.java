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

import java.util.HashSet;
import java.util.Set;

/**
 * Provides the constant name for the file format properties
 */
public class FileFormatProperties {
  private static final Set<String> SUPPORTED_EXTERNAL_FORMAT = new HashSet<String>() {
    {
      add("csv");
      add("kafka");
    }
  };

  public static boolean isExternalFormatSupported(String format) {
    return SUPPORTED_EXTERNAL_FORMAT.contains(format.toLowerCase());
  }
  public static Set<String> getSupportedExternalFormat() {
    return SUPPORTED_EXTERNAL_FORMAT;
  }

  public static class CSV {
    public static final String HEADER = "csv.header";
    public static final String DELIMITER = "csv.delimiter";
    public static final String COMMENT = "csv.comment";
    public static final String SKIP_EMPTY_LINE = "csv.skipemptyline";
    public static final String QUOTE = "csv.quote";
    public static final String ESCAPE = "csv.escape";
  }
}
