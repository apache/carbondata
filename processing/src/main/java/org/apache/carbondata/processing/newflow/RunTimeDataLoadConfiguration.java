package org.apache.carbondata.processing.newflow;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;

/**
 * It contains run time properties which can be passed from one step to other step.
 */
public class RunTimeDataLoadConfiguration {

  private SegmentProperties segmentProperties;

  private int[] dimLenghts;

  public SegmentProperties getSegmentProperties() {
    return segmentProperties;
  }

  public void setSegmentProperties(SegmentProperties segmentProperties) {
    this.segmentProperties = segmentProperties;
  }

  public int[] getDimLenghts() {
    return dimLenghts;
  }

  public void setDimLenghts(int[] dimLenghts) {
    this.dimLenghts = dimLenghts;
  }
}
