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

package org.apache.carbondata.core.statusmanager;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/*
  Prior to Carbon 1.3 the the loadMetaData @timestamp and @loadStartTime was stored as
  as date string format "dd-MM-yyyy HH:mm:ss:SSS". The date string value is specific
  to the timezone. SO the timestamp in long by the date string will not result into
  same value if converted to date in different timezone.
  Json Object of LoadMetaData before CarbonData 1.3
 |-------------------------------------------------------------------------------------------|
 | [{"timestamp":"15-12-2017 16:50:31:703","loadStatus":"Success","loadName":"0",            |
 | "partitionCount":"0","isDeleted":"FALSE","dataSize":"912","indexSize":"700",              |
 | "updateDeltaEndTimestamp":"","updateDeltaStartTimestamp":"","updateStatusFileName":"",    |
 | "loadStartTime":"15-12-2017 16:50:27:493","visibility":"true","fileFormat":"COLUMNAR_V3"}]|
 |-------------------------------------------------------------------------------------------|
  Fix: As the System.currentTimeMillis() returns the same value irrespective of timezone.
  So if Carbon stores the long value for @timestamp  and @loadStartTime value then the
  value will be same irrespective of the timezone.
  Json Object of LoadMetaData for CarbonData 1.3
 |-------------------------------------------------------------------------------------------|
 | [{"timestamp":"1513336827593","loadStatus":"Success","loadName":"0",                      |
 | "partitionCount":"0","isDeleted":"FALSE","dataSize":"912","indexSize":"700",              |
 | "updateDeltaEndTimestamp":"","updateDeltaStartTimestamp":"","updateStatusFileName":"",    |
 | "loadStartTime":"1513336827593","visibility":"true","fileFormat":"COLUMNAR_V3"}]          |
 |-------------------------------------------------------------------------------------------|
 */
public class LoadMetadataDetails implements Serializable {

  private static final long serialVersionUID = 1106104914918491724L;
  private String timestamp;

  // For backward compatibility, this member is required to read from JSON in the table_status file
  private SegmentStatus loadStatus;

  // name of the segment
  private String loadName;

  private String isDeleted = CarbonCommonConstants.KEYWORD_FALSE;
  private String dataSize;
  private String indexSize;

  public String getDataSize() {
    return dataSize;
  }

  public void setDataSize(String dataSize) {
    this.dataSize = dataSize;
  }

  public String getIndexSize() {
    return indexSize;
  }

  public void setIndexSize(String indexSize) {
    this.indexSize = indexSize;
  }

  // update delta end timestamp
  private String updateDeltaEndTimestamp = "";

  // update delta start timestamp
  private String updateDeltaStartTimestamp = "";

  // this will represent the update status file name at that point of time.
  private String updateStatusFileName = "";

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LoadMetadataDetails.class.getName());

  // dont remove static as the write will fail.
  private static final SimpleDateFormat parser =
      new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS);
  /**
   * Segment modification or deletion time stamp
   */
  private String modificationOrdeletionTimesStamp;
  private String loadStartTime;

  private String mergedLoadName;
  /**
   * visibility is used to determine whether to the load is visible or not.
   */
  private String visibility = "true";

  /**
   * To know if the segment is a major compacted segment or not.
   */
  private String majorCompacted;

  /**
   * the file format of this segment
   */
  private FileFormat fileFormat = FileFormat.COLUMNAR_V3;

  /**
   * Segment file name where it has the information of partition information.
   */
  private String segmentFile;

  public long getLoadEndTime() {
    if (timestamp == null) {
      return CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT;
    }
    return convertTimeStampToLong(timestamp);
  }

  public void setLoadEndTime(long timestamp) {
    this.timestamp = Long.toString(timestamp);
  }

  public SegmentStatus getSegmentStatus() {
    return loadStatus;
  }

  public void setSegmentStatus(SegmentStatus segmentStatus) {
    this.loadStatus = segmentStatus;
  }

  public String getLoadName() {
    return loadName;
  }

  public void setLoadName(String loadName) {
    this.loadName = loadName;
  }

  /**
   * @return the modificationOrdeletionTimesStamp
   */
  public long getModificationOrdeletionTimesStamp() {
    if (null == modificationOrdeletionTimesStamp) {
      return 0;
    }
    return convertTimeStampToLong(modificationOrdeletionTimesStamp);
  }

  /**
   * @param modificationOrdeletionTimesStamp the modificationOrdeletionTimesStamp to set
   */
  public void setModificationOrdeletionTimesStamp(long modificationOrdeletionTimesStamp) {
    this.modificationOrdeletionTimesStamp =
        Long.toString(modificationOrdeletionTimesStamp);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((loadName == null) ? 0 : loadName.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override public boolean equals(Object obj) {
    if (obj == null) {
      return false;

    }
    if (!(obj instanceof LoadMetadataDetails)) {
      return false;
    }
    LoadMetadataDetails other = (LoadMetadataDetails) obj;
    if (loadName == null) {
      if (other.loadName != null) {
        return false;
      }
    } else if (!loadName.equals(other.loadName)) {
      return false;
    }
    return true;
  }

  /**
   * @return the startLoadTime
   */
  public long getLoadStartTime() {
    if (loadStartTime == null) {
      return CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT;
    }
    return convertTimeStampToLong(loadStartTime);
  }

  /**
   * return loadStartTime
   *
   * @return
   */
  public long getLoadStartTimeAsLong() {
    if (!loadStartTime.isEmpty()) {
      Long time = getTimeStamp(loadStartTime);
      if (null != time) {
        return time;
      }
    }
    return 0;
  }

  /**
   * This method will convert a given timestamp to long value and then to string back
   *
   * @param factTimeStamp
   * @return Long    TimeStamp value is milliseconds
   */
  private long convertTimeStampToLong(String factTimeStamp) {
    try {
      return Long.parseLong(factTimeStamp);
    } catch (NumberFormatException nf) {
      SimpleDateFormat parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS);
      // it is the processing for existing table before carbon 1.3
      Date dateToStr = null;
      try {
        dateToStr = parser.parse(factTimeStamp);
        return dateToStr.getTime();
      } catch (ParseException e) {
        LOGGER
            .error("Cannot convert" + factTimeStamp + " to Time/Long type value" + e.getMessage());
        parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
        try {
          // if the load is in progress, factTimeStamp will be null, so use current time
          if (null == factTimeStamp) {
            return System.currentTimeMillis();
          }
          dateToStr = parser.parse(factTimeStamp);
          return dateToStr.getTime();
        } catch (ParseException e1) {
          LOGGER.error(
              "Cannot convert" + factTimeStamp + " to Time/Long type value" + e1.getMessage());
          return 0;
        }
      }
    }
  }

  /**
   * returns load start time as long value
   *
   * @param loadStartTime
   * @return Long  TimeStamp value is nanoseconds
   */
  public Long getTimeStamp(String loadStartTime) {
    try {
      return Long.parseLong(loadStartTime) * 1000L;
    } catch (NumberFormatException nf) {
      // it is the processing for existing table before carbon 1.3
      Date dateToStr = null;
      try {
        dateToStr = parser.parse(loadStartTime);
        return dateToStr.getTime() * 1000;
      } catch (ParseException e) {
        LOGGER.error("Cannot convert" + loadStartTime +
            " to Time/Long type value" + e.getMessage());
        return null;
      }
    }
  }

  /**
   * @param loadStartTime
   */
  public void setLoadStartTime(long loadStartTime) {
    this.loadStartTime = Long.toString(loadStartTime);
  }

  /**
   * @return the mergedLoadName
   */
  public String getMergedLoadName() {
    return mergedLoadName;
  }

  /**
   * @param mergedLoadName the mergedLoadName to set
   */
  public void setMergedLoadName(String mergedLoadName) {
    this.mergedLoadName = mergedLoadName;
  }

  /**
   * @return the visibility
   */
  public String getVisibility() {
    return visibility;
  }

  /**
   * @param visibility the visibility to set
   */
  public void setVisibility(String visibility) {
    this.visibility = visibility;
  }

  /**
   * Return true if it is a major compacted segment.
   * @return majorCompacted
   */
  public String isMajorCompacted() {
    return majorCompacted;
  }

  /**
   * Set true if it is a major compacted segment.
   *
   * @param majorCompacted
   */
  public void setMajorCompacted(String majorCompacted) {
    this.majorCompacted = majorCompacted;
  }

  /**
   * To set isDeleted property.
   *
   * @param isDeleted
   */
  public void setIsDeleted(String isDeleted) {
    this.isDeleted = isDeleted;
  }

  /**
   * To get the update delta end timestamp
   *
   * @return updateDeltaEndTimestamp
   */
  public String getUpdateDeltaEndTimestamp() {
    return updateDeltaEndTimestamp;
  }

  /**
   * To set the update delta end timestamp
   *
   * @param updateDeltaEndTimestamp
   */
  public void setUpdateDeltaEndTimestamp(String updateDeltaEndTimestamp) {
    this.updateDeltaEndTimestamp = updateDeltaEndTimestamp;
  }

  /**
   * To get the update delta start timestamp
   *
   * @return updateDeltaStartTimestamp
   */
  public String getUpdateDeltaStartTimestamp() {
    return updateDeltaStartTimestamp;
  }

  /**
   * To set the update delta start timestamp
   *
   * @param updateDeltaStartTimestamp
   */
  public void setUpdateDeltaStartTimestamp(String updateDeltaStartTimestamp) {
    this.updateDeltaStartTimestamp = updateDeltaStartTimestamp;
  }

  /**
   * To get the updateStatusFileName
   *
   * @return updateStatusFileName
   */
  public String getUpdateStatusFileName() {
    return updateStatusFileName;
  }

  /**
   * To set the updateStatusFileName
   *
   * @param updateStatusFileName
   */
  public void setUpdateStatusFileName(String updateStatusFileName) {
    this.updateStatusFileName = updateStatusFileName;
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  public String getSegmentFile() {
    return segmentFile;
  }

  public void setSegmentFile(String segmentFile) {
    this.segmentFile = segmentFile;
  }

  @Override public String toString() {
    return "LoadMetadataDetails{" + "loadStatus=" + loadStatus + ", loadName='" + loadName + '\''
        + ", loadStartTime='" + loadStartTime + '\'' + ", segmentFile='" + segmentFile + '\'' + '}';
  }
}
