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

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class stores the blocklet details of delete delta file
 */
public class DeleteDeltaBlockletDetails implements Serializable {

  private static final long serialVersionUID = 1206104914911491724L;
  private String id;
  private Integer pageId;

  private Set<Integer> deletedRows;

  public DeleteDeltaBlockletDetails(String id, Integer pageId) {
    this.id = id;
    deletedRows = new TreeSet<Integer>();
    this.pageId = pageId;
  }

  public boolean addDeletedRows(Set<Integer> rows) {
    return deletedRows.addAll(rows);
  }

  public boolean addDeletedRow(Integer row) {
    return deletedRows.add(row);
  }

  public Integer getPageId() {
    return pageId;
  }

  public Set<Integer> getDeletedRows() {
    return deletedRows;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !(obj instanceof DeleteDeltaBlockletDetails)) {
      return false;
    }
    DeleteDeltaBlockletDetails that = (DeleteDeltaBlockletDetails) obj;
    if (null == pageId || null == that.pageId) {
      return false;
    }
    return id.equals(that.id) && (pageId.intValue() == that.pageId.intValue());
  }

  @Override public int hashCode() {
    return id.hashCode() + pageId.hashCode();
  }

  public String getBlockletKey() {
    return this.id + '_' + this.pageId;
  }

}
