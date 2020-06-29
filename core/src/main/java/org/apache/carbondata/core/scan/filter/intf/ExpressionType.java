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

package org.apache.carbondata.core.scan.filter.intf;

public enum ExpressionType {

  AND,
  OR,
  NOT,
  EQUALS,
  NOT_EQUALS,
  LESSTHAN,
  LESSTHAN_EQUALTO,
  GREATERTHAN,
  GREATERTHAN_EQUALTO,
  ADD,
  SUBSTRACT,
  DIVIDE,
  MULTIPLY,
  IN,
  LIST,
  NOT_IN,
  UNKNOWN,
  LITERAL,
  RANGE,
  FALSE,
  TRUE,
  STARTSWITH,
  ENDSWITH,
  CONTAINSWITH,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3217
  TEXT_MATCH,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3548
  IMPLICIT
}
