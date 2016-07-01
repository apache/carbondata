/*
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

package org.carbondata.scan.expression;

public enum DataType {
  StringType(0), DateType(1), TimestampType(2), BooleanType(1), IntegerType(3), FloatType(
      4), LongType(5), DoubleType(6), NullType(7), DecimalType(8), ArrayType(9), StructType(
      10), ShortType(11);
  private int presedenceOrder;

  private DataType(int value) {
    this.presedenceOrder = value;
  }

  public int getPresedenceOrder() {
    return presedenceOrder;
  }
}
