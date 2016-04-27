package org.carbondata.query.carbon.result;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

public class ListBasedResultWrapper {

  private ByteArrayWrapper key;

  private MeasureAggregator[] value;

  /**
   * @return the key
   */
  public ByteArrayWrapper getKey() {
    return key;
  }

  /**
   * @param key the key to set
   */
  public void setKey(ByteArrayWrapper key) {
    this.key = key;
  }

  /**
   * @return the value
   */
  public MeasureAggregator[] getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(MeasureAggregator[] value) {
    this.value = value;
  }
}
