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

package org.apache.carbondata.core.mutate;

/**
 * Enum class for tupleID.
 */
public enum TupleIdEnum {
  PARTITION_PART_ID(0),
  SEGMENT_ID(0),
  EXTERNAL_SEGMENT_ID(1),
  PARTITION_SEGMENT_ID(1),
  BLOCK_ID(1),
  EXTERNAL_BLOCK_ID(2),
  BLOCKLET_ID(2),
  PAGE_ID(3),
  OFFSET(4),
  EXTERNAL_BLOCKLET_ID(3),
  EXTERNAL_PAGE_ID(4),
  EXTERNAL_OFFSET(5);

  private int index;

  TupleIdEnum(int index) {
    this.index = index;
  }

  public int getTupleIdIndex() {
    return this.index;
  }

}
