package org.carbondata.core.carbon.datastore.block;

import java.util.List;

import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter;

public abstract class AbstractIndex {

  /**
   * vo class which will hold the RS information of the block
   */
  protected SegmentProperties segmentProperties;

  /**
   * data block
   */
  protected DataRefNode dataRefNode;

  /**
   * total number of row present in the block
   */
  protected long totalNumberOfRows;

  /**
   * @return the totalNumberOfRows
   */
  public long getTotalNumberOfRows() {
    return totalNumberOfRows;
  }

  /**
   * @return the segmentProperties
   */
  public SegmentProperties getSegmentProperties() {
    return segmentProperties;
  }

  /**
   * @return the dataBlock
   */
  public DataRefNode getDataRefNode() {
    return dataRefNode;
  }

  /**
   * Below method will be used to load the data block
   *
   * @param blockInfo block detail
   */
  public abstract void buildIndex(List<DataFileFooter> footerList);
}
