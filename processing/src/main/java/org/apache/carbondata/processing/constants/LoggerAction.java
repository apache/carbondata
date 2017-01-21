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

package org.apache.carbondata.processing.constants;

/**
 * enum to hold the bad record logger action
 */
public enum LoggerAction {

  FORCE("FORCE"), // data will be converted to null
  REDIRECT("REDIRECT"), // no null conversion moved to bad record and written to raw csv
  IGNORE("IGNORE"); // no null conversion moved to bad record and not written to raw csv

  private String name;

  LoggerAction(String name) {
    this.name = name;
  }

  @Override public String toString() {
    return this.name;
  }
}
