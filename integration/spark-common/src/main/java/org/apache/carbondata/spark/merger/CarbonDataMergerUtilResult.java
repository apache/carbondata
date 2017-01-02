package org.apache.carbondata.spark.merger;

import org.apache.carbondata.core.update.SegmentUpdateDetails;

public final class CarbonDataMergerUtilResult extends SegmentUpdateDetails {
  private boolean compactionStatus;

  public boolean getCompactionStatus() {return compactionStatus;}

  public void setCompactionStatus(Boolean status) {compactionStatus = status;}

}
