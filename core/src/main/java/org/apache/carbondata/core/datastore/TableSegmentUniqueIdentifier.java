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

package org.apache.carbondata.core.datastore;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

/**
 * Class holds the absoluteTableIdentifier and segmentId to uniquely identify a segment
 */
public class TableSegmentUniqueIdentifier {
  /**
   * table fully qualified identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;

  private String segmentId;

  /**
   * Constructor to initialize the class instance
   * @param absoluteTableIdentifier
   * @param segmentId
   */
  public TableSegmentUniqueIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier,
      String segmentId) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
    this.segmentId = segmentId;
  }

  public String getSegmentId() {
    return segmentId;
  }

  /**
   * method returns the id to uniquely identify a key
   *
   * @return
   */
  public String getUniqueTableSegmentIdentifier() {
    CarbonTableIdentifier carbonTableIdentifier =
            absoluteTableIdentifier.getCarbonTableIdentifier();
    return carbonTableIdentifier.getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR
            + carbonTableIdentifier.getTableName() + CarbonCommonConstants.UNDERSCORE
            + carbonTableIdentifier.getTableId() + CarbonCommonConstants.FILE_SEPARATOR + segmentId;
  }

  /**
   * equals method to compare two objects having same
   * absoluteIdentifier and segmentId
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableSegmentUniqueIdentifier that = (TableSegmentUniqueIdentifier) o;

    if (!absoluteTableIdentifier.equals(that.absoluteTableIdentifier)) return false;
    return segmentId.equals(that.segmentId);

  }

  /**
   * Returns hashcode for the TableSegmentIdentifier
   * @return
   */
  @Override
  public int hashCode() {
    int result = absoluteTableIdentifier.hashCode();
    result = 31 * result + segmentId.hashCode();
    return result;
  }
}
