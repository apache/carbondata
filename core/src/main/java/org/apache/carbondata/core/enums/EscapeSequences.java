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

package org.apache.carbondata.core.enums;

public enum EscapeSequences {

  NEW_LINE("\\n", '\n'), BACKSPACE("\\b", '\b'), TAB("\\t", '\t'), CARRIAGE_RETURN("\\r", '\r');

  /**
   * name of the function
   */
  private String name;

  /**
   * unicode of the escape char
   */
  private char escapeChar;

  EscapeSequences(String name, char escapeChar) {
    this.name = name;
    this.escapeChar = escapeChar;
  }

  public String getName() {
    return this.name;
  }

  public String getEscapeChar() {
    return String.valueOf(this.escapeChar);
  }

}
