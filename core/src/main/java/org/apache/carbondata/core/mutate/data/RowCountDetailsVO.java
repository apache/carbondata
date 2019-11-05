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

package org.apache.carbondata.core.mutate.data;

import java.io.Serializable;

/**
 * VO class Details for block.
 */
public class RowCountDetailsVO implements Serializable {

  private static final long serialVersionUID = 1206104914918491749L;

  private long totalNumberOfRows;

  private long deletedRowsInBlock;

  public RowCountDetailsVO(long totalNumberOfRows, long deletedRowsInBlock) {
    this.totalNumberOfRows = totalNumberOfRows;
    this.deletedRowsInBlock = deletedRowsInBlock;
  }

  public long getTotalNumberOfRows() {
    return totalNumberOfRows;
  }

  public long getDeletedRowsInBlock() {
    return deletedRowsInBlock;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    RowCountDetailsVO that = (RowCountDetailsVO) obj;

    if (totalNumberOfRows != that.totalNumberOfRows) {
      return false;
    }
    return deletedRowsInBlock == that.deletedRowsInBlock;

  }

  @Override
  public int hashCode() {
    int result = (int) (totalNumberOfRows ^ (totalNumberOfRows >>> 32));
    result = 31 * result + (int) (deletedRowsInBlock ^ (deletedRowsInBlock >>> 32));
    return result;
  }
}
