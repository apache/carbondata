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

package org.apache.carbondata.core.metadata.schema.table;

import java.util.List;

public class BucketFields {
  private List<String> bucketColumns;
  private int numberOfBuckets;

  public BucketFields(List<String> bucketColumns, String numberOfBuckets)
      throws MalformedCarbonCommandException {
    this.bucketColumns = bucketColumns;
    try {
      this.numberOfBuckets = Integer.parseInt(numberOfBuckets);
      if (this.numberOfBuckets <= 0) {
        throw new MalformedCarbonCommandException("BUCKETNUMBER must be positive value");
      }
    } catch (NumberFormatException e) {
      throw new MalformedCarbonCommandException("BUCKETNUMBER must be integer value");
    }
  }

  public List<String> getBucketColumns() {
    return bucketColumns;
  }

  public int getNumberOfBuckets() {
    return numberOfBuckets;
  }
}
