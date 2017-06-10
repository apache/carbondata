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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

/**
 * This class stores the blocklet details of delete delta file
 */
public class DeleteDeltaBlockletDetails implements Serializable {

  private static final long serialVersionUID = 1206104914911491724L;
  private String id;
  private Integer pageId;

  private Set<Integer> deletedRows;

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteDeltaBlockletDetails.class.getName());

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

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
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
    return id.equals(that.id) && pageId == that.pageId;
  }

  @Override public int hashCode() {
    return id.hashCode();
  }

}
