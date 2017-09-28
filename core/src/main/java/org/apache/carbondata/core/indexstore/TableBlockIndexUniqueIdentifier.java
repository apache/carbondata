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

package org.apache.carbondata.core.indexstore;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

/**
 * Class holds the absoluteTableIdentifier and segmentId to uniquely identify a segment
 */
public class TableBlockIndexUniqueIdentifier {
  /**
   * table fully qualified identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;

  private String segmentId;

  private String carbonIndexFileName;

  /**
   * Constructor to initialize the class instance
   *
   * @param absoluteTableIdentifier
   * @param segmentId
   */
  public TableBlockIndexUniqueIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier,
      String segmentId, String carbonIndexFileName) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
    this.segmentId = segmentId;
    this.carbonIndexFileName = carbonIndexFileName;
  }

  /**
   * returns AbsoluteTableIdentifier
   *
   * @return
   */
  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
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
        + carbonTableIdentifier.getTableId() + CarbonCommonConstants.FILE_SEPARATOR + segmentId
        + CarbonCommonConstants.FILE_SEPARATOR + carbonIndexFileName;
  }

  public String getFilePath() {
    return absoluteTableIdentifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId + "/"
        + carbonIndexFileName;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableBlockIndexUniqueIdentifier that = (TableBlockIndexUniqueIdentifier) o;

    if (!absoluteTableIdentifier.equals(that.absoluteTableIdentifier)) {
      return false;
    }
    if (!segmentId.equals(that.segmentId)) {
      return false;
    }
    return carbonIndexFileName.equals(that.carbonIndexFileName);
  }

  @Override public int hashCode() {
    int result = absoluteTableIdentifier.hashCode();
    result = 31 * result + segmentId.hashCode();
    result = 31 * result + carbonIndexFileName.hashCode();
    return result;
  }

  public String getCarbonIndexFileName() {
    return carbonIndexFileName;
  }
}
